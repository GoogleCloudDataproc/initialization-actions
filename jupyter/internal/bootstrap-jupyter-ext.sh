#!/bin/bash
set -e
sudo apt-get install -y git

# Installs Jupyter notebook extensions and RISE
# https://github.com/ipython-contrib/IPython-notebook-extensions

# 0. Ensure local Jupyter / IPython (~/.local/) dir exists.

# Simple hack to circumvent exception when .local directory does not yet exist:
# > OSError: [Errno 2] No such file or directory:
# > '/home/vagrant/.local/share/jupyter'
# > Command failed: /bin/bash -x -e
# > /home/vagrant/miniconda-3.18.3/conda-bld/work/conda_build.sh
# If directory doesn't exist, attempt to create it.
if [[ ! -d "~/.local/share/jupyter/" ]]
then
  echo "~/.local/share/jupyter/ directory does not exist, creating..."
  mkdir -p ~/.local/share/jupyter/
  echo "~/.local/share/jupyter/ directory created, continuing..."
fi

# 1. Install IPyNB extensions:
nbext_path='IPython-notebook-extensions'
if [[ ! -d $nbext_path ]]
then
    echo "Installing $nbext_path"
    git clone https://github.com/ipython-contrib/IPython-notebook-extensions.git
    conda build IPython-notebook-extensions
    conda install --use-local nbextensions
else
    echo "Existing directory at path: $nbext_path, skipping install!"
fi

# 2. Install RISE (http://github.com/damianavila/RISE)
rise_path='RISE'
if [[ ! -d $rise_path ]]
then
    echo "Installing $rise_path"
    git clone https://github.com/damianavila/RISE.git
    cd RISE && python setup.py install
else
    echo "Existing directory at path: $rise_path, skipping install!"
fi

