#!/bin/bash
set -x -e

# 0. Set parameters
# TODO: Detect if directory exists in working directory
NOTEBOOK_DIR="./notebooks"
source /root/.bashrc


# 1. Ensure PySpark configuration exists
#  /root/.ipython/profile_default/startup/00-pyspark-setup.py
IPYTHON_STARTUP_CONFIG_PATH="/dataproc_intialization_actions/jupyter/00_setup_pyspark.py"
IPYTHON_STARTUP_PATH="/root/.ipython/profile_default/startup/"

# 2. Install and launch Jupyter (only run on the master node)
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then

    # 1. Install Jupyter
    # Assumes conda is already installed and available in $PATH
    conda install jupyter

    # 2. Ensure PySpark is configured
    if [[ ! -d $IPYTHON_STARTUP_PATH ]]; then
        echo "IPython startup config path not detected, creating..."
        mkdir -p $IPYTHON_STARTUP_CONFIG_PATH
        echo "Copying IPython config from data proc init action repo..."
        cp -p $IPYTHON_STARTUP_CONFIG_PATH $IPYTHON_STARTUP_PATH
    else
        echo "IPython startup path detected, skipping..."
    fi

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


