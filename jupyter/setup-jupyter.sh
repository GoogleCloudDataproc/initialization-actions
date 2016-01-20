#!/bin/bash
set -x -e

source /root/.bashrc

# 1. Ensure PySpark configuration exists
#  /root/.ipython/profile_default/startup/00-pyspark-setup.py
IPYTHON_STARTUP_CONFIG_PATH="/dataproc-initialization-actions/jupyter/00_setup_pyspark.py"
IPYTHON_STARTUP_PATH="/root/.ipython/profile_default/startup/"

# 2. Install and launch Jupyter (only run on the master node)
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then

    # 1. Install Jupyter
    #TODO: Detect of conda env is running, select proper install command.
    # Assumes conda or pip is already installed and available in $PATH
    #pip install jupyter

    if [[ ! -v CONDA_BIN_PATH ]]; then
        source /etc/profile.d/conda_config.sh  #/$HOME/.bashrc
    fi
    conda install jupyter

    # 2. Ensure PySpark is configured
    if [[ ! -d $IPYTHON_STARTUP_PATH ]]; then
        echo "IPython startup config path not detected, creating..."
        mkdir -p $IPYTHON_STARTUP_PATH
        echo "Copying IPython config from data proc init action repo..."
        cp -p $IPYTHON_STARTUP_CONFIG_PATH $IPYTHON_STARTUP_PATH
    else
        echo "IPython startup path detected, skipping..."
    fi
fi
echo "Jupyter setup...!"



