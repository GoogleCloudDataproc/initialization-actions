#!/usr/bin/env bash
set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "$DIR" ]] && DIR="$PWD"

source "$DIR/../util/utils.sh"
[[ ! -v CONDA_BIN_PATH ]] && source /etc/profile.d/conda_config.sh

echo "Starting Jupyter notebook..."
nohup jupyter notebook --no-browser > /var/log/jupyter_notebook.log 2>&1 &

