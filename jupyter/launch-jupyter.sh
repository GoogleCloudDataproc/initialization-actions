#!/usr/bin/env bash
set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "$DIR" ]] && DIR="$PWD"

source "$DIR/util/utils.sh"

# 0. Set parameters
# TODO: Detect if directory exists in working directory
NOTEBOOK_DIR="/root/notebooks"
JUPYTER_PORT=$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT)
JUPYTER_IP=*

[[ ! $JUPYTER_PORT =~ ^[0-9]+$ ]] && throw "metadata must contain a valid 'JUPITER_PORT' value, but instead has the value \"$JUPYTER_PORT\""

if [[ ! -v CONDA_BIN_PATH ]]; then
    source /etc/profile.d/conda_config.sh  #/$HOME/.bashrc
fi

# 2. Launch Jupyter notebook on port 8123
if [[ ! -d $NOTEBOOK_DIR ]]; then
    nohup jupyter notebook --no-browser --ip=$JUPYTER_IP --port=$JUPYTER_PORT > /var/log/jupyter_notebook.log &
else
    nohup jupyter notebook --notebook-dir=$NOTEBOOK_DIR --no-browser --ip=$JUPYTER_IP --port=$JUPYTER_PORT > /var/log/jupyter_notebook.log &
fi
echo "Jupyter notebook launched on $JUPYTER_IP:$JUPYTER_PORT!"
