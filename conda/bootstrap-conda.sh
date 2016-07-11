#!/bin/bash
set -e
# Modified from bootstrap-conda.sh script, see:
# https://gist.github.com/nehalecky/aea2100ca8bad83fe974

# Run on BOTH 'Master' and 'Worker' nodes
#ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
#if [[ "${ROLE}" == 'Master' ]]; then
MINICONDA_VARIANT=$(/usr/share/google/get_metadata_value attributes/MINICONDA_VARIANT || true)

if [[ ! -v CONDA_INSTALL_PATH ]]; then
    echo "CONDA_INSTALL_PATH not set, setting ..."
    CONDA_INSTALL_PATH="/usr/local/bin/miniconda"
    echo "Set CONDA_INSTALL_PATH to $CONDA_INSTALL_PATH"
fi
# 0. Specify Miniconda version
## 0.1 A few parameters
## specify base operating system
if [[ ! -v OS_TYPE ]]; then
    echo "OS_TYPE not set, setting  ..."
    OS_TYPE="Linux-x86_64.sh"
    echo "Set OS_TYPE to $OS_TYPE"
fi
## Python 2 or 3?
if [[ ! -v MINICONDA_VARIANT ]]; then
    echo "MINICONDA_VARIANT not set, setting ... "
    MINICONDA_VARIANT="Miniconda3"  #for Python 3.5.x
    echo "Set MINICONDA_VARIANT to $MINICONDA_VARIANT"
else
    MINICONDA_VARIANT="Miniconda$MINICONDA_VARIANT"
fi

## specify Miniconda release (e.g., MINICONDA_VER='4.0.5')
if [[ ! -v MINICONDA_VER ]]; then
    echo "MINICONDA_VER not set, setting ..."
    MINICONDA_VER='latest'
    set "Set MINICONDA_VER to $MINICONDA_VER"
fi

## 0.2 Compute Miniconda version
miniconda="$MINICONDA_VARIANT-$MINICONDA_VER-$OS_TYPE"
echo "Miniconda version specified: $miniconda"
## 0.3 Set MD5 hash for check (if desired)
#expectedHash="b1b15a3436bb7de1da3ccc6e08c7a5df"

# 1. Setup Miniconda Install
## 1.1 Define Miniconda install directory
echo "Working directory: $PWD"
if [[ ! -v $PROJ_DIR ]]; then
    echo "No path argument specified, setting install directory as working directory: $PWD."
    PROJ_DIR=$PWD
fi

## 1.2 Setup Miniconda
cd $PROJ_DIR
MINICONDA_SCRIPT_PATH="$PROJ_DIR/$miniconda"
echo "Defined miniconda script path: $MINICONDA_SCRIPT_PATH"

if [[ -f "$MINICONDA_SCRIPT_PATH" ]]; then
  echo "Found existing Miniconda script at: $MINICONDA_SCRIPT_PATH"
else
  echo "Downloading Miniconda script to: $MINICONDA_SCRIPT_PATH ..."
  wget http://repo.continuum.io/miniconda/$miniconda -P "$PROJ_DIR"
  echo "Downloaded $miniconda!"
  ls -al $MINICONDA_SCRIPT_PATH
  chmod 755 $MINICONDA_SCRIPT_PATH
fi

## 1.3 #md5sum hash check of miniconda installer
if [[ -v expectedHash ]]; then
    md5Output=$(md5sum $MINICONDA_SCRIPT_PATH | awk '{print $1}')
    if [ "$expectedHash" != "$md5Output" ]; then
        echo "Unexpected md5sum $md5Output for $miniconda"
        exit 1
    fi
fi

# 2. Install conda
## 2.1 Via bootstrap
LOCAL_CONDA_PATH="$PROJ_DIR/miniconda"
if [[ ! -d $LOCAL_CONDA_PATH ]]; then
    #blow away old symlink / default Miniconda install
    rm -rf "$PROJ_DIR/miniconda"
    # Install Miniconda
    echo "Installing $miniconda to $CONDA_INSTALL_PATH..."
    bash $MINICONDA_SCRIPT_PATH -b -p $CONDA_INSTALL_PATH -f
    chmod 755 $CONDA_INSTALL_PATH
    #create symlink
    ln -sf $CONDA_INSTALL_PATH "$PROJ_DIR/miniconda"
    chmod 755 "$PROJ_DIR/miniconda"
else
    echo "Existing directory at path: $LOCAL_CONDA_PATH, skipping install!"
fi

## 2.2 Update PATH and conda...
echo "Setting environment variables..."
CONDA_BIN_PATH="$CONDA_INSTALL_PATH/bin"
export PATH="$CONDA_BIN_PATH:$PATH"
echo "Updated PATH: $PATH"
echo "And also HOME: $HOME"
hash -r
which conda
conda config --set always_yes true --set changeps1 false

echo "Updating conda..."
conda update -q conda
# Useful for debugging any issues with conda
conda info -a

# Install useful conda utilities in root env
echo "Installing useful conda utilities in root env..."
conda install anaconda-client conda-build

conda update --all

# 2.3 Update global profiles to add the miniconda location to PATH and PYTHONHASHSEED
# based on: http://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux
# and also: http://askubuntu.com/questions/391515/changing-etc-environment-did-not-affect-my-environemtn-variables
# and this: http://askubuntu.com/questions/128413/setting-the-path-so-it-applies-to-all-users-including-root-sudo
echo "Updating global profiles to export miniconda bin location to PATH and set PYTHONHASHSEED ..."
#if grep -ir "CONDA_BIN_PATH=$CONDA_BIN_PATH" /root/.bashrc  #/$HOME/.bashrc
if [[ -f "/etc/profile.d/conda_config.sh" ]]
    then
    echo "conda_config.sh found in /etc/profile.d/, skipping..."
else
    echo "Adding path definition to profiles..."
    echo "export CONDA_BIN_PATH=$CONDA_BIN_PATH" | tee -a /etc/profile.d/conda_config.sh  /etc/*bashrc /etc/profile
    echo 'export PATH=$CONDA_BIN_PATH:$PATH' | tee -a /etc/profile.d/conda_config.sh  /etc/*bashrc /etc/profile
    # Fix issue with Python3 hash seed.
    # Issue here: https://issues.apache.org/jira/browse/SPARK-12100
    # Fix here: http://blog.stuart.axelbrooke.com/python-3-on-spark-return-of-the-pythonhashseed/
    echo "Adding PYTHONHASHSEED=0 to profiles and spark-defaults.conf..."
    echo "export PYTHONHASHSEED=0" | tee -a  /etc/profile.d/conda_config.sh  /etc/*bashrc  /usr/lib/spark/conf/spark-env.sh
    echo "spark.executorEnv.PYTHONHASHSEED=0" >> /etc/spark/conf/spark-defaults.conf
fi

## 3. Ensure that Anaconda Python and PySpark play nice
### http://blog.cloudera.com/blog/2015/09/how-to-prepare-your-apache-hadoop-cluster-for-pyspark-jobs/
echo "Ensure that Anaconda Python and PySpark play nice by all pointing to same Python distro..."
if [[ ! -v PYSPARK_PYTHON ]]; then  echo "export PYSPARK_PYTHON=$CONDA_BIN_PATH/python" | tee -a  /etc/profile.d/conda_config.sh  /etc/*bashrc /etc/environment /usr/lib/spark/conf/spark-env.sh; fi

echo "Finished bootstrapping via Miniconda, sourcing /etc/profile ..."
source /etc/profile
