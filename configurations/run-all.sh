#!/bin/bash
# Start Spark master UI in background
/opt/bitnami/scripts/spark/run.sh &

# Jalankan Jupyter Lab di foreground (biar container tetap hidup)
# Jalankan Jupyter Lab tanpa token di foreground
exec jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token='' \
    --NotebookApp.disable_check_xsrf=True \
    --notebook-dir=/app