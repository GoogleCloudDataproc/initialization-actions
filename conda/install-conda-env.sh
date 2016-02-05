#!/bin/bash
set -e

# 0. Specify Packages to be installed
## 0.1 conda packages to be installed
CONDA_PACKAGES='numpy pandas scikit-learn networkx seaborn bokeh ipython Jupyter pytables'
## 0.2 pip packages to be installed
PIP_PACKAGES='plotly py4j'

echo "echo \$USER: $USER"
echo "echo \$PWD: $PWD"
echo "echo \$PATH: $PATH"
echo "echo \$CONDA_BIN_PATH: $CONDA_BIN_PATH"

if [[ ! -v CONDA_BIN_PATH ]]; then
    source /etc/profile.d/conda_config.sh  #/$HOME/.bashrc
fi

echo "echo \$USER: $USER"
echo "echo \$PWD: $PWD"
echo "echo \$PATH: $PATH"
echo "echo \$CONDA_BIN_PATH: $CONDA_BIN_PATH"

# 1. Specify conda environment name
if [ $# -eq 0 ]
    then
    echo "No conda environment name specified, setting to 'root' env..."
    CONDA_ENV_NAME='root'
else
    echo "conda environment name specified, setting to: $1"
    CONDA_ENV_NAME=$1
fi

# 1. Create conda env and install conda packages
echo "Attempting to create conda environment: $CONDA_ENV_NAME"
if conda info --envs | grep -q $CONDA_ENV_NAME
    then
    echo "conda environment $CONDA_ENV_NAME detected, skipping env creation..."
    echo "Activating $CONDA_ENV_NAME environment..."
    source activate $CONDA_ENV_NAME
    echo "Installing / updating conda packages: $CONDA_PACKAGES"
    conda install $CONDA_PACKAGES
    pip install $PIP_PACKAGES
else
    echo "Creating conda environment and installing conda packages..."
    echo "Installing CONDA_PACKAGES for $CONDA_ENV_NAME..."
    echo "conda packages requested: $CONDA_PACKAGES"
    conda create -q -n $CONDA_ENV_NAME $CONDA_PACKAGES || true
    echo "conda environment $CONDA_ENV_NAME created..."
    pip install $PIP_PACKAGES
fi

# 2. Append .bashrc with source activate
echo "Attempting to append .bashrc to activate conda env at login..."
if grep -ir "source activate $CONDA_ENV_NAME" /$HOME/.bashrc
    then
    echo "conda env activation found in .bashrc, skipping..."
else
    echo "Appending .bashrc to activate conda env at login.."
    sudo echo "source activate $CONDA_ENV_NAME"         >> $HOME/.bashrc
    echo ".bashrc successfully appended!"
fi

echo "Activating $CONDA_ENV_NAME environment..."
source activate $CONDA_ENV_NAME


