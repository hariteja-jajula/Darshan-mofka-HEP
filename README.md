# darshan-mofka-HEP

Stream **Darshan** I/O events out of a running **HEP GN2 / SALT** GPU training
job into **Mofka**, consume them with **FlowCept**, and reconstruct a native
`.darshan` log from the live stream — measuring the overhead the streaming path
adds to a realistic ML training workload.

This is a self-contained, minimal snapshot of the larger `darshan-mofka`
harness, curated down to exactly what the **HEP/SALT** workload needs so it can be
shared and reproduced on its own.

---

## Why

Darshan normally writes its characterization log only at application shutdown. If
a job crashes, is killed, or never finalizes, that log is lost. This project adds
a Darshan runtime connector that **publishes I/O events while the job is still
running**; the stream is later reconstructed into a standard `.darshan` log.

The HEP/SALT case is the stress test: a real GPU training run (GN2 jet-flavour
tagging) under Apptainer, with the custom Darshan `LD_PRELOAD`ed *inside* the
container. The overhead study compares three arms:

| Arm | Darshan runtime | Mofka streaming | What it isolates |
|-----|-----------------|-----------------|------------------|
| `baseline`    | off | off | the workload with no instrumentation |
| `runtimeonly` | on  | off | cost of Darshan interception alone |
| `streaming`   | on  | on  | cost of interception **+** live push to Mofka |

`streaming − runtimeonly` is the streaming overhead; `runtimeonly − baseline` is
the plain Darshan overhead.

---

## Architecture

```text
  HEP GN2 / SALT training  (salt fit, inside salt-dev.sif, --nv GPU)
        │
        │  LD_PRELOAD = libdarshan.so   (custom Darshan runtime)
        ▼
  Darshan runtime  ── intercepts POSIX/STDIO/HDF5 I/O calls
        │
        │  darshan-mofka.c connector: per I/O op → inline JSON event
        ▼
  diaspora-stream-api  (C/C++ producer)  ──►  Mofka broker  (bedrock: flock /
        │                                     yokan / warabi / abt_io / mofka,
        │                                     ofi+cxi transport, MPMD layout)
        ▼
  FlowCept consumer  ── drains the Mofka topic ──►  MongoDB
        │
        ▼
  export_jsonl.py  ──►  events.jsonl
        │
        ▼
  darshan-mofka-reconstruct  ──►  streamed/*.darshan   (native-style logs)
        │
        ▼
  strict_compare.py  ──  streamed log  vs  the run's native .darshan log
```

All three tiers (broker, FlowCept consumer, SALT workload) are launched in a
**single MPMD `mpiexec`** so they share one Slingshot job VNI on Polaris. The
connector pushes **directly** to Diaspora — there is no custom ring buffer or
drain thread.

### The connector

The custom code that makes this work is small and lives in the vendored Darshan
fork:

```text
darshan/darshan-runtime/lib/darshan-mofka.c    the Mofka connector
darshan/darshan-runtime/lib/darshan-mofka.h
darshan/darshan-runtime/lib/darshan-core.c      hook into shutdown/flush path
darshan/darshan-util/darshan-mofka-reconstruct.c stream → .darshan reconstructor
```

Each intercepted I/O op is serialized to a compact JSON event
(`type, schema, schema_version, module, op, record_id, rank, len, started_at,
ended_at`) and handed to `diaspora_producer_push()`. Push latency is timed and a
summary (`init_us`, `pushes`, `push_avg_us`) is printed at finalize.

---

## Repository layout

```text
darshan/              Darshan fork with the Mofka connector      (vendored source)
diaspora-stream-api/  C/C++ streaming API the connector links    (vendored source)
flowcept/             consumer that drains Mofka into MongoDB     (vendored source)

env/                  cluster/runtime environment (Polaris profile)
lib/                  run.sh (MPMD orchestration) + config.sh (YAML reader)
server/               bedrock broker config, spack stack recipe, server helpers
Client/               FlowCept consumer launcher + JSON export
overhead_study/       submission library + driver + queue helpers
workloads/            job.sh (the single runner) + HEP/ launcher + test inputs
install/              source-build fallback for the Mofka/Mochi dependency stack
build.sh              builds the custom Darshan runtime (libdarshan.so)
check-deps.sh         read-only dependency checker
PROVENANCE.md         exact upstream commits the vendored trees came from
```

### Not included (large / external artifacts — see below to obtain)

- `workloads/HEP/salt-dev.sif` — the **8.1 GB** SALT container image.
- The built dependency stack (Mofka/Mochi/bedrock via Spack), the Python venv,
  and all Darshan/diaspora build outputs (`*/install`, `*/_build*`). These are
  produced by the build steps below.

The small SALT **test inputs** (`workloads/HEP/dummy_data/{train,val}.h5`,
`norm_dict.yaml`, `class_dict.yaml`, ~24 MB) **are** included so a run is
immediately reproducible once the container is in place.

---

## Reproduce (Polaris / ALCF)

Target platform is **Polaris** (PBS, `ofi+cxi`, `gcc-native/12.3`). LCRC/Improv
is supported by the same scripts via `ENV_PROFILE=lcrc`.

### 0. Prerequisites

- ALCF Polaris account with a project (`PBS_ACCOUNT=<project>`).
- Access to `home` + `eagle` filesystems.
- Apptainer/Singularity (loaded via module inside the run scripts).
- A working Spack (the recipe below pins one).

### 1. Get the code

```bash
# this repository (already self-contained: the three forks are vendored source)
git clone <this-repo-url> darshan-mofka-HEP
cd darshan-mofka-HEP
```

The `darshan/`, `diaspora-stream-api/`, and `flowcept/` trees are vendored plain
source (see `PROVENANCE.md` for the exact upstream commits). No
`git submodule update` is needed.

### 2. Build the Mofka / Mochi dependency stack

The connector links against Mofka + the Mochi stack (margo, thallium, mercury,
argobots, yokan, warabi, flock, bedrock). Build it once via the pinned Spack
environment:

```bash
# Polaris
DARSHAN_MOFKA_PROFILE=polaris bash install/setup.sh
# (LCRC/Improv:  DARSHAN_MOFKA_PROFILE=lcrc bash install/setup.sh)
```

The exact concretization is pinned in `server/spack/spack.lock`
(`spack.yaml` / `spack-lcrc.yaml` are the editable specs; `patches/` holds the
required mercury + mofka patches). This step also builds `diaspora-stream-api`
(C API + Python) and creates the Python venv FlowCept runs in.

After it completes, source the environment so the following builds see it:

```bash
source env/server.sh      # sets PY, PYTHONPATH, LD_LIBRARY_PATH, MOFKA_SPACK_VIEW
source env/workload.sh     # sets DIASPORA_C, DARSHAN_PREFIX
```

### 3. Build the custom Darshan runtime + utils

```bash
# runtime -> darshan/install/lib/libdarshan.so  (this is what gets LD_PRELOAD'd)
bash build.sh

# util tools (darshan-parser + darshan-mofka-reconstruct) are built automatically
# by workloads/job.sh on first use. To build them by hand (same steps job.sh uses):
( cd darshan && ./prepare.sh
  cd darshan-util && mkdir -p _build_util
  cd _build_util && ../configure --prefix="$PWD/../install" && make -j4 && make install )
```

Verify:

```bash
bash check-deps.sh
ls darshan/install/lib/libdarshan.so
```

### 4. Obtain the SALT container image

`workloads/HEP/salt-dev.sif` (~8.1 GB) is **not** in this repo. It is an
Apptainer image of the SALT training framework (`umami-hep/salt`, the GN2
configs) with a CUDA-enabled PyTorch. Obtain it one of two ways:

**a) Ask the author for the prebuilt image** (fastest — it is the exact image the
measurements were taken with) and place it at:

```text
workloads/HEP/salt-dev.sif
```

**b) Rebuild it** from the upstream SALT project. On a machine where you have
build/root (or `--fakeroot`) rights:

```bash
# pull the SALT framework and build a container with GPU PyTorch + salt installed.
# See https://github.com/umami-hep/salt for the current install instructions;
# the image must provide the `salt` CLI and mount the training tree at
# /workspace/salt (run_salt_streaming.sh runs:
#   salt fit --config /workspace/salt/salt/configs/GN2/GN2.yaml ...)
apptainer build salt-dev.sif <your-salt.def>
mv salt-dev.sif workloads/HEP/salt-dev.sif
```

The container needs: the `salt` CLI on `PATH`, GN2 configs under
`/workspace/salt/salt/configs/GN2/`, and PyTorch built for the Polaris GPUs. The
custom `libdarshan.so` is injected into the container at run time via
`APPTAINERENV_PREPEND_LD_LIBRARY_PATH` + `LD_PRELOAD` — it does **not** need to be
baked into the image.

### 5. Run the overhead study

Dry-run first (prints the exact `qsub` lines, submits nothing):

```bash
DRYRUN=1 PBS_ACCOUNT=<project> bash workloads/HEP/run_HEP.sh
```

Then submit the three arms:

```bash
PBS_ACCOUNT=<project> bash workloads/HEP/run_HEP.sh
```

Knobs: `REPS=<n>` (reps per arm), `STUDY=<name>` (results subdir),
`ARMS="baseline runtimeonly streaming"` (subset of arms).

> **Polaris queue note:** `debug` (≤2 nodes) and `debug-scaling` (>2 nodes) each
> allow only **1 running + 1 queued** job per user. Use
> `overhead_study/dripfeed.sh` to feed a batch of runs through that limit.

### 6. Results

Each rep lands under `results/<STUDY>/<arm>/RUN<n>/`:

```text
events.jsonl           the drained stream (FlowCept → Mongo → export_jsonl.py)
streamed/*.darshan     logs reconstructed from the stream
*.darshan              the run's native Darshan log (for comparison)
TELEMETRY / config.txt run metadata + streamed-vs-native verdict
```

`workloads/strict_compare.py` diffs the reconstructed log against the native one;
`overhead_study/extract_all.sh` aggregates timings across arms/reps.

---

## Notes

- **No ring buffer.** The connector pushes each event straight to Diaspora; there
  is deliberately no custom in-process queue or drain thread.
- **MPMD / single VNI.** Broker, consumer, and workload share one `mpiexec` on
  Polaris so they land on one Slingshot job VNI (`ofi+cxi`).
- **Shared, immutable stack.** The Mofka/Mochi/bedrock stack is built once and
  reused read-only; only the Darshan and diaspora libraries are rebuilt during
  development.

See `env/README.md`, `workloads/README.md`, `server/spack/README.md`, and
`install/README.md` for component-level detail.
