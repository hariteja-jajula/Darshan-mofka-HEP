#!/bin/bash
#
# HEP/SALT streaming overhead study -- submission launcher.
#
# Submits the three-arm overhead study (baseline / runtimeonly / streaming) for
# the HEP GN2 (SALT) training workload over Mofka (ofi+cxi, MPMD layout).
# Self-locates the repo root, sets the HEP knobs, and hands off to the shared
# submit library, which qsubs workloads/job.sh per arm.
#
# Knobs (override via env): REPS, STUDY, DRYRUN=1 (print qsub, do not submit).
#
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

WLNODES=1
SRVNODES=1

WORKLOAD=hep-salt
PROTO=ofi+cxi
TASKS=1

REPS="${REPS:-1}"

PARTITIONS=4
CONSUMERS=1

# Reuse an existing custom Darshan/Mofka build (see build.sh); do not rebuild per run.
SKIP_BUILD=1

STUDY="${STUDY:-HEP_SALT_smoke}"

source "$ROOT/overhead_study/_submit_lib.sh"
