#!/bin/bash
set -euo pipefail

ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
HEP="$ROOT/workloads/HEP"

SIF="$HEP/salt-dev.sif"
DARSHAN_LIB="${DARSHAN_LIB_SO:-$ROOT/darshan/install/lib/libdarshan.so}"

mkdir -p \
    "$HEP/dummy_data" \
    "$HEP/darshan_logs" \
    "$HEP/logs"

# ---------------------------------------------------------------------------
# Required inputs from the EXISTING streaming harness.
# Do not create a separate Mofka configuration here.
# ---------------------------------------------------------------------------

: "${DARSHAN_MOFKA_GROUP_FILE:?DARSHAN_MOFKA_GROUP_FILE was not supplied by streaming harness}"

TOPIC="${DARSHAN_MOFKA_TOPIC:-darshan}"
ENABLE="${DARSHAN_MOFKA_ENABLE:-1}"
TIMING="${DARSHAN_MOFKA_TIMING:-1}"
FLUSH_MS="${DARSHAN_MOFKA_FLUSH_MS:-30000}"
MAX_BATCHES="${DARSHAN_MOFKA_MAX_BATCHES:-512}"
SENDER_THREADS="${DIASPORA_C_SENDER_THREADS:-1}"

if [ ! -f "$SIF" ]; then
    echo "ERROR: missing SALT image: $SIF" >&2
    exit 1
fi

if [ ! -f "$DARSHAN_LIB" ]; then
    echo "ERROR: missing custom Darshan: $DARSHAN_LIB" >&2
    exit 1
fi

if [ ! -r "$DARSHAN_MOFKA_GROUP_FILE" ]; then
    echo "ERROR: Mofka group file not readable: $DARSHAN_MOFKA_GROUP_FILE" >&2
    exit 1
fi

echo "=== HEP/SALT streaming workload ==="
echo "SIF=$SIF"
echo "DARSHAN_LIB=$DARSHAN_LIB"
echo "DARSHAN_MOFKA_GROUP_FILE=$DARSHAN_MOFKA_GROUP_FILE"
echo "DARSHAN_MOFKA_TOPIC=$TOPIC"


STACK_LD="$ROOT/darshan/install/lib:${LD_LIBRARY_PATH:-}"

BINDS=(
    -B "$ROOT:$ROOT"
    -B "$HEP/dummy_data:/workspace/dummy_data"
    -B "$HEP/darshan_logs:/workspace/darshan_logs"
    -B "$HEP/logs:/workspace/logs"
)

# Run from HEP so SALT retains the same working-directory behavior as
# the already-tested manual execution.
cd "$HEP"

# ---------------------------------------------------------------------------
# Smoke validation: GPU + custom Darshan dependencies.
#
# IMPORTANT:
# LD_PRELOAD is explicitly removed from the host-side Apptainer launcher.
# The custom Darshan preload is supplied only to the contained SALT process.
# ---------------------------------------------------------------------------

echo "=== HEP smoke validation ==="

env -u LD_PRELOAD -u BASH_ENV -u ENV \
    APPTAINERENV_PREPEND_LD_LIBRARY_PATH="$STACK_LD" \
    apptainer exec \
        --nv \
        --fakeroot \
        "${BINDS[@]}" \
        "$SIF" \
        env PYTHONNOUSERSITE=1 \
        bash -c '
            python - <<'"'"'PY'"'"'
import torch

print("torch version:", torch.__version__)
print("torch CUDA build:", torch.version.cuda)
print("torch path:", torch.__file__)
print("cuda available:", torch.cuda.is_available())

if torch.cuda.is_available():
    print("GPU:", torch.cuda.get_device_name(0))
PY
        '

echo "=== custom Darshan dependencies inside container ==="

env -u LD_PRELOAD -u BASH_ENV -u ENV \
    APPTAINERENV_PREPEND_LD_LIBRARY_PATH="$STACK_LD" \
    apptainer exec \
        --nv \
        --fakeroot \
        "${BINDS[@]}" \
        "$SIF" \
        bash --noprofile --norc -c "
            echo 'Darshan library: $DARSHAN_LIB'
            echo
            ldd '$DARSHAN_LIB'
        "

# ---------------------------------------------------------------------------
# Environment explicitly passed into SALT.
# ---------------------------------------------------------------------------

APPT_ENV=(
    --env "PYTHONNOUSERSITE=1"
    --env "DARSHAN_MOFKA_GROUP_FILE=$DARSHAN_MOFKA_GROUP_FILE"
    --env "DARSHAN_MOFKA_TOPIC=$TOPIC"
    --env "DARSHAN_MOFKA_ENABLE=$ENABLE"
    --env "DARSHAN_MOFKA_TIMING=$TIMING"
    --env "DARSHAN_MOFKA_FLUSH_MS=$FLUSH_MS"
    --env "DARSHAN_MOFKA_MAX_BATCHES=$MAX_BATCHES"
    --env "DIASPORA_C_SENDER_THREADS=$SENDER_THREADS"
)

# Baseline = no Darshan at all.
# Runtime-only + streaming = custom Darshan preload.
if [ "${NO_DARSHAN:-0}" != 1 ]; then
    APPT_ENV+=(
        --env "LD_PRELOAD=$DARSHAN_LIB"
        --env "DARSHAN_LD_PRELOAD=$DARSHAN_LIB"
    )
fi

# Preserve these knobs only if the existing harness supplied them.
if [ -n "${DARSHAN_MOFKA_BATCH:-}" ]; then
    APPT_ENV+=(
        --env "DARSHAN_MOFKA_BATCH=$DARSHAN_MOFKA_BATCH"
    )
fi

if [ -n "${DARSHAN_MOFKA_VERBOSE:-}" ]; then
    APPT_ENV+=(
        --env "DARSHAN_MOFKA_VERBOSE=$DARSHAN_MOFKA_VERBOSE"
    )
fi

if [ -n "${DARSHAN_MOFKA_FINAL_SWEEP:-}" ]; then
    APPT_ENV+=(
        --env "DARSHAN_MOFKA_FINAL_SWEEP=$DARSHAN_MOFKA_FINAL_SWEEP"
    )
fi

echo "=== launching SALT/GN2 ==="

# Do not let a host LD_PRELOAD affect the Apptainer executable itself.
# Pass custom libdarshan.so only into the container.
env -u LD_PRELOAD -u BASH_ENV -u ENV \
    APPTAINERENV_PREPEND_LD_LIBRARY_PATH="$STACK_LD" \
    apptainer exec \
        --nv \
        --fakeroot \
        "${BINDS[@]}" \
        "${APPT_ENV[@]}" \
        "$SIF" \
        bash -c '
            echo "inside container:"
            echo "  PYTHONNOUSERSITE=$PYTHONNOUSERSITE"
            echo "  LD_PRELOAD=${LD_PRELOAD:-<unset>}"
            echo "  DARSHAN_MOFKA_GROUP_FILE=$DARSHAN_MOFKA_GROUP_FILE"
            echo "  DARSHAN_MOFKA_TOPIC=$DARSHAN_MOFKA_TOPIC"

            exec salt fit --force \
                --config /workspace/salt/salt/configs/GN2/GN2.yaml \
                --trainer.devices=1 \
                --data.batch_size=64 \
                --data.num_workers=4 \
                --data.persistent_workers=true \
                --data.multiprocessing_context=spawn \
                --data.train_file=/workspace/dummy_data/train.h5 \
                --data.val_file=/workspace/dummy_data/val.h5 \
                --data.norm_dict=/workspace/dummy_data/norm_dict.yaml \
                --data.class_dict=/workspace/dummy_data/class_dict.yaml \
                --data.num_train=1000 \
                --data.num_val=200 \
                --trainer.max_epochs=2 \
                --name=GN2_streaming
        '