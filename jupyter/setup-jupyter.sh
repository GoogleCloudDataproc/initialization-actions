#!/bin/bash
set -x -e

# 0. Set parameters
# TODO: Detect if directory exists in working directory
NOTEBOOK_DIR="./notebooks"

# 1. Ensure pyspark configuration exists
IPYTHON_STARTUP_PATH="~/.ipython/profile_default/startup/"

if [[ ! -d $IPYTHON_STARTUP_PATH ]]; then
    mkdir -p ~/.ipython/profile_default/startup/
else
    echo "IPython startup path detected, skipping..."
fi

# 2. Install and launch Jupyter (only run on the master node)
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then

    # 1. Install Jupyter
    # Assumes conda is already installed and available in $PATH
    conda install jupyter

	# 2. Launch Jupyter notebook on port 8123
	if [[ ! -d $NOTEBOOK_DIR ]]; then
        nohup jupyter notebook \
            --no-browser \
            --ip=* \
            --port=8123 \
            > /var/log/python_notebook.log &
    else
	    nohup jupyter notebook \
	        --notebook-dir=$NOTEBOOK_DIR \
	        --no-browser \
	        --ip=* \
	        --port=8123 \
	        > /var/log/python_notebook.log &
    fi

fi

echo "Jupyter notebook launched on port 8123!"


