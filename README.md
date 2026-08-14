# darshan-mofka-HEP

Run the HEP/SALT training workload with Darshan I/O events streamed to Mofka.

## 1. Build the stack

```bash
DARSHAN_MOFKA_PROFILE=polaris bash install/setup.sh   # Mofka/Mochi + diaspora-c + venv
source env/server.sh
source env/workload.sh
bash build.sh                                          # custom libdarshan.so
```

## 2. Get the SALT container

Place the SALT image here (ask the author, or build from https://github.com/umami-hep/salt):

```bash
workloads/HEP/salt-dev.sif
```

## 3. Run

```bash
DRYRUN=1 PBS_ACCOUNT=<project> bash workloads/HEP/run_HEP.sh   # preview qsub
PBS_ACCOUNT=<project> bash workloads/HEP/run_HEP.sh            # submit
```

Results land in `results/<STUDY>/<arm>/RUN<n>/` (`events.jsonl`, reconstructed
`streamed/*.darshan`, native `*.darshan`). Knobs: `REPS`, `STUDY`, `ARMS`.
