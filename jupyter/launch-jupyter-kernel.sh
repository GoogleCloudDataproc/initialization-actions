#!/usr/bin/env bash
set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "$DIR" ]] && DIR="$PWD"

source "$DIR/../util/utils.sh"

if [[ ! -v CONDA_BIN_PATH ]]; then
    source /etc/profile.d/conda_config.sh  #/$HOME/.bashrc
fi

echo "Starting Jupyter notebook..."
nohup jupyter notebook --no-browser > /var/log/jupyter_notebook.log &

