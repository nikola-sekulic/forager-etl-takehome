#!/usr/bin/env bash
# Read the persisted measurements from the most recent completed pipeline run.
set -euo pipefail
# Keep container paths intact when invoked from Git Bash on Windows.
export MSYS_NO_PATHCONV=1

cd "$(dirname "$0")/.."
extra=()
if es_peak=$(docker compose exec -T elasticsearch sh -c '
    if [ -r /sys/fs/cgroup/memory.peak ]; then
        cat /sys/fs/cgroup/memory.peak
    elif [ -r /sys/fs/cgroup/memory/memory.max_usage_in_bytes ]; then
        cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes
    else
        exit 1
    fi
' 2>/dev/null); then
    if [[ "$es_peak" =~ ^[0-9]+$ ]] && (( es_peak > 0 )); then
        extra=(--es-peak-bytes "$es_peak")
    fi
fi
docker compose run --rm --no-deps pipeline python /app/bench/report.py "${extra[@]}"
