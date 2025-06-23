#!/bin/bash

# Tambahkan PYTHONPATH hanya untuk script ini
export PYTHONPATH="/app:/app/src:/opt/bitnami/spark/python/"

LOG_DIR="/app/logs"
mkdir -p "$LOG_DIR"

LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d).log"
echo "[INFO] Starting pipeline at $(date)" >> "$LOG_FILE"
echo "[INFO] PYTHONPATH=$PYTHONPATH" >> "$LOG_FILE"

# Jalankan pipeline
python /app/pipeline.py >> "$LOG_FILE" 2>&1

echo "[INFO] Finished pipeline at $(date)" >> "$LOG_FILE"
