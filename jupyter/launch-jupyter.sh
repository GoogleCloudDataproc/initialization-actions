#!/bin/bash
set -x -e

# 0. Set parameters
# TODO: Detect if directory exists in working directory
NOTEBOOK_DIR="/root/notebooks"

# 2. Launch Jupyter notebook on port 8123
if [[ ! -d $NOTEBOOK_DIR ]]; then
    nohup jupyter notebook --no-browser --ip=* --port=8123 | tee -a /var/log/jupyter_notebook.log &
else
    nohup jupyter notebook --notebook-dir=$NOTEBOOK_DIR --no-browser --ip=* --port=8123 | tee -a /var/log/jupyter_notebook.log &
fi
echo "Jupyter notebook launched on port 8123!"