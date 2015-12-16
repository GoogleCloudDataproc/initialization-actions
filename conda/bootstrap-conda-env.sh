#!/bin/bash
set -e

# 0. Set parameters
source ./bin/common.sh

# 1. Create conda env and install conda packages
echo "Attempting to create conda environment: $conda_env_name"
if conda info --envs | grep -q $conda_env_name
    then
    echo "conda environment $conda_env_name detected, skipping env creation..."
    echo "Activating $conda_env_name environment..."
    source activate $conda_env_name
    echo "Installing / updating conda packages: $conda_packages"
    conda install $conda_packages
else
    echo "Creating conda environment and installing conda packages..."
    echo "Installing conda_packages for $conda_env_name..."
    echo "conda packages requested: $conda_packages"
    conda create -q -n $conda_env_name $conda_packages || true
    echo "conda environment $conda_env_name created..."

fi

# 2. Append .bashrc with source activate
echo "Attempting to append .bashrc to activate conda env at login..."
if grep -ir "source activate $conda_env_name" /$HOME/.bashrc
    then
    echo "conda env activation found in .bashrc, skipping..."
else
    echo "Appending .bashrc to activate conda env at login.."
    sudo echo "source activate $conda_env_name"         >> $HOME/.bashrc
    echo ".bashrc successfully appended!"
fi

echo "Activating $conda_env_name environment..."
source activate $conda_env_name


