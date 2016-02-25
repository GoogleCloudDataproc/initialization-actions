#!/bin/bash
set -x -e

# 0. Set parameters
# TODO: Detect if directory exists in working directory
NOTEBOOK_DIR="/root/notebooks"
JUPYTER_PORT=8123
JUPYTER_IP=*

# 2. Launch Jupyter notebook on port 8123
if [[ ! -d $NOTEBOOK_DIR ]]; then
    nohup jupyter notebook --no-browser --ip=$JUPYTER_IP --port=$JUPYTER_PORT > /var/log/jupyter_notebook.log &
else
    nohup jupyter notebook --notebook-dir=$NOTEBOOK_DIR --no-browser --ip=$JUPYTER_IP --port=$JUPYTER_PORT > /var/log/jupyter_notebook.log &
fi
echo "Jupyter notebook launched on $JUPYTER_IP:$JUPYTER_PORT!"
