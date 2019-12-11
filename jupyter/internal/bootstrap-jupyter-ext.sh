#!/bin/bash

# This script installs Jupyter extensions

set -euxo pipefail

readonly NBEXT_PATH='IPython-notebook-extensions'
readonly RISE_PATH='RISE'

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

update_apt_get
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
if [[ ! -d "~/.local/share/jupyter/" ]]; then
  echo "~/.local/share/jupyter/ directory does not exist, creating..."
  mkdir -p ~/.local/share/jupyter/
  echo "~/.local/share/jupyter/ directory created, continuing..."
fi

# 1. Install IPyNB extensions:
if [[ ! -d "${NBEXT_PATH}" ]]; then
  echo "Installing ${NBEXT_PATH}"
  git clone https://github.com/ipython-contrib/IPython-notebook-extensions.git
  conda build IPython-notebook-extensions
  conda install --use-local nbextensions
else
  echo "Existing directory at path: ${NBEXT_PATH}, skipping install!"
fi

# 2. Install RISE (http://github.com/damianavila/RISE)
if [[ ! -d "${RISE_PATH}" ]]; then
  echo "Installing ${RISE_PATH}"
  git clone https://github.com/damianavila/RISE.git
  cd "${RISE_PATH}" && python setup.py install
else
  echo "Existing directory at path: ${RISE_PATH}, skipping install!"
fi
