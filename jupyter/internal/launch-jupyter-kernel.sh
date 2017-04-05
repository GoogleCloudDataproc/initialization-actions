#!/usr/bin/env bash
set -e

echo "Starting Jupyter notebook..."
nohup jupyter notebook --allow-root --no-browser > /var/log/jupyter_notebook.log 2>&1 &

