#!/bin/bash

# Only run on the master node
#ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
#if [[ "${ROLE}" == 'Master' ]]; then

# Run on BOTH 'Master' and 'Worker' nodes

# 0. Specify Miniconda version
# 0.1 A few parameters
# specify base operating system
OS_TYPE="Linux-x86_64.sh"
# specify Miniconda release
MINICONDA_VER='3.18.3'
## Python 2 or 3?
#MINICONDA_VARIANT="Miniconda2"  #for Python 2.7.x
#expectedHash="3f1d3e33dd154aacd7367931d595a74c"
MINICONDA_VARIANT="Miniconda3"  #for Python 3.5.x
expectedHash="245a839794fe2c3e7a35691ae1d3033f"

## 0.2 Compute Miniconda version
miniconda="$MINICONDA_VARIANT-$MINICONDA_VER-$OS_TYPE"

# 1. Setup Miniconda Install
## 1.1 Define Miniconda install directory
echo "Working direcotry: $PWD"
if [ $# -eq 0 ]
    then
    echo "No path argument specified, setting install directory as working directory: $PWD."
    PROJ_DIR=$PWD

else
    echo "Path argument specified, installing to: $1"
    PROJ_DIR=$1
fi

## 1.2 Setup Miniconda
cd $PROJ_DIR
PATH_INSTALL_BIN_DIR="$PROJ_DIR/vm/shared/bin"
PATH_CONDA_SCRIPT="$PATH_INSTALL_BIN_DIR/$miniconda"
echo "Defined miniconda script path: $PATH_CONDA_SCRIPT"

if [[ -f "$PATH_CONDA_SCRIPT" ]]; then
  echo "Found existing Miniconda script at: $PATH_CONDA_SCRIPT"
else
  echo "Downloading Miniconda script to: $PATH_CONDA_SCRIPT ..."
  wget http://repo.continuum.io/miniconda/$miniconda -P "$PATH_INSTALL_BIN_DIR"
  echo "Downloaded $miniconda!"
  ls -al $PATH_CONDA_SCRIPT
  chmod 755 $PATH_CONDA_SCRIPT
fi

# 1.3 #md5sum hash check of miniconda installer
md5Output=$(md5sum $PATH_CONDA_SCRIPT | awk '{print $1}')
if [ "$expectedHash" != "$md5Output" ]; then
    echo "Unexpected md5sum $md5Output for $miniconda"
    exit 1
fi

# 2. Install Miniconda
## 2.1 Via bootstrap
PATH_CONDA="$PROJ_DIR/miniconda-$MINICONDA_VARIANT"
if [[ ! -d $PATH_CONDA ]]; then
    #blow away old symlink / default miniconda install
    rm -rf "$PROJ_DIR/miniconda"
    # Install Miniconda
    echo "Installing $miniconda to $PATH_CONDA..."
    bash $PATH_CONDA_SCRIPT -b -p $PATH_CONDA -f
    chmod 755 $PATH_CONDA
    #create symlink
    ln -sf $PATH_CONDA "$PROJ_DIR/miniconda"
    chmod 755 "$PROJ_DIR/miniconda"
else
    echo "Existing directory at path: $PATH_CONDA, skipping install!"
fi

# 2.2 Update PATH and conda...
echo "Setting environment variables..."
PATH_CONDA_BIN="$PATH_CONDA/bin"
export PATH="$PATH_CONDA_BIN:$PATH"
echo "Updated PATH: $PATH"
echo "And also HOME: $HOME"
hash -r
which conda
conda config --set always_yes yes --set changeps1 no
source ~/.bashrc

echo "Updating conda..."
conda update -q conda
# Useful for debugging any issues with conda
conda info -a

# Install useful conda utilities in root env
conda install pip anaconda-client conda-build conda-env
conda install -n root -c conda conda-env

# 2.3 Update .bashrc profile to add the miniconda location to PATH.
if grep -ir "PATH_CONDA_BIN=" /$HOME/.bashrc
    then
    echo "Path environment variable definition found in .bashrc, skipping..."
else
    echo "Adding path definintion to .bashrc..."
    echo "export PATH_CONDA_BIN=$PATH_CONDA_BIN"        >> $HOME/.bashrc
    sudo echo 'export PATH=$PATH:$PATH_CONDA_BIN'       >> $HOME/.bashrc
fi
echo "Updated PATH: $PATH"

#fi
