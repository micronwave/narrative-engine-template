#!/usr/bin/env bash
# Pipeline runner — configure cron for desired frequency.
# Recommended: every 4 hours (6x/day). Set PIPELINE_FREQUENCY_HOURS in .env.
# Example crontab: 0 */4 * * * /path/to/run_pipeline.sh

set -euo pipefail
cd "$(dirname "$0")"

mkdir -p logs

# Prevent concurrent execution
LOCKFILE="pipeline.lock"
if [ -f "$LOCKFILE" ]; then
    echo "$(date) Pipeline already running, skipping." >> logs/pipeline_skip.log
    exit 0
fi
trap 'rm -f "$LOCKFILE"' EXIT
date > "$LOCKFILE"

LOGFILE="logs/pipeline_$(date +%Y%m%d_%H%M).log"
python pipeline.py >> "$LOGFILE" 2>&1
