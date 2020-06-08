#!/bin/bash

set -exo pipefail

readonly NOT_SUPPORTED_MESSAGE="Conda initialization action is not supported on Dataproc 2.0+.
Use Anaconda Component instead: https://cloud.google.com/dataproc/docs/concepts/components/anaconda"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Modified from bootstrap-conda.sh script, see:
# https://bitbucket.org/bombora-datascience/bootstrap-conda

# Run on BOTH 'Master' and 'Worker' nodes
#ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
#if [[ "${ROLE}" == 'Master' ]]; then

MINICONDA_VARIANT=$(/usr/share/google/get_metadata_value attributes/MINICONDA_VARIANT || true)
MINICONDA_VERSION=$(/usr/share/google/get_metadata_value attributes/MINICONDA_VERSION || true)

OLD_MINICONDA_VERSION="4.2.12"
NEW_MINICONDA_VERSION="4.7.10"
MIN_SPARK_VERSION_FOR_NEWER_MINICONDA="2.2.0"

if [[ -f /etc/profile.d/effective-python.sh ]]; then
  PROFILE_SCRIPT_PATH=/etc/profile.d/effective-python.sh
else
  PROFILE_SCRIPT_PATH=/etc/profile.d/conda.sh
fi

if [[ ! -v CONDA_INSTALL_PATH ]]; then
  echo "CONDA_INSTALL_PATH not set, setting ..."
  CONDA_INSTALL_PATH="/opt/conda"
  echo "Set CONDA_INSTALL_PATH to $CONDA_INSTALL_PATH"
fi

# Check if Conda is already installed at the expected location. This will allow
# this init action to override the default Miniconda in 1.4+ or Anaconda
# optional component in 1.3+ which is installed at /opt/conda/default.
if [[ -f "${CONDA_INSTALL_PATH}/bin/conda" ]]; then
  echo "Dataproc has installed Conda previously at ${CONDA_INSTALL_PATH}. Skipping install!"
  exit 0
else
  # 0. Specify Miniconda version
  ## 0.1 A few parameters
  ## specify base operating system
  if [[ ! -v OS_TYPE ]]; then
    echo "OS_TYPE not set, setting  ..."
    OS_TYPE="Linux-x86_64.sh"
    echo "Set OS_TYPE to $OS_TYPE"
  fi
  ## Python 2 or 3 based miniconda?
  if [[ -z "${MINICONDA_VARIANT}" ]]; then
    echo "MINICONDA_VARIANT not set, setting ... "
    MINICONDA_VARIANT="3" #for Python 3.5.x
    echo "Set MINICONDA_VARIANT to $MINICONDA_VARIANT"
  fi
  ## specify Miniconda release (e.g., MINICONDA_VERSION='4.0.5')
  if [[ -z "${MINICONDA_VERSION}" ]]; then
    # Pin to 4.2.12 by default until Spark default is 2.2.0, then use newer
    # one (https://issues.apache.org/jira/browse/SPARK-19019)
    SPARK_VERSION=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\.[0-9]\+\+\).*/\1/p' | head -n1)
    if dpkg --compare-versions ${SPARK_VERSION} ge ${MIN_SPARK_VERSION_FOR_NEWER_MINICONDA}; then
      MINICONDA_VERSION="${NEW_MINICONDA_VERSION}"
    else
      MINICONDA_VERSION="${OLD_MINICONDA_VERSION}"
    fi
  fi

  ## 0.2 Compute Miniconda version
  MINICONDA_FULL_NAME="Miniconda$MINICONDA_VARIANT-$MINICONDA_VERSION-$OS_TYPE"
  echo "Complete Miniconda version resolved to: $MINICONDA_FULL_NAME"
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
  MINICONDA_SCRIPT_PATH="$PROJ_DIR/$MINICONDA_FULL_NAME"
  echo "Defined Miniconda script path: $MINICONDA_SCRIPT_PATH"

  if [[ -f "$MINICONDA_SCRIPT_PATH" ]]; then
    echo "Found existing Miniconda script at: $MINICONDA_SCRIPT_PATH"
  else
    echo "Downloading Miniconda script to: $MINICONDA_SCRIPT_PATH ..."
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      https://repo.continuum.io/miniconda/$MINICONDA_FULL_NAME -P "$PROJ_DIR"
    echo "Downloaded $MINICONDA_FULL_NAME!"
    ls -al $MINICONDA_SCRIPT_PATH
    chmod 755 $MINICONDA_SCRIPT_PATH
  fi

  ## 1.3 #md5sum hash check of miniconda installer
  if [[ -v expectedHash ]]; then
    md5Output=$(md5sum $MINICONDA_SCRIPT_PATH | awk '{print $1}')
    if [ "$expectedHash" != "$md5Output" ]; then
      echo "Unexpected md5sum $md5Output for $MINICONDA_FULL_NAME"
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
    echo "Installing $MINICONDA_FULL_NAME to $CONDA_INSTALL_PATH..."
    bash $MINICONDA_SCRIPT_PATH -b -p $CONDA_INSTALL_PATH -f
    chmod 755 $CONDA_INSTALL_PATH
    #create symlink
    ln -sf $CONDA_INSTALL_PATH "$PROJ_DIR/miniconda"
    chmod 755 "$PROJ_DIR/miniconda"
  else
    echo "Existing directory at path: $LOCAL_CONDA_PATH, skipping install!"
  fi
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

# Useful printout for debugging any issues with conda
conda info -a

## 2.3 Update global profiles to add the miniconda location to PATH
# based on: http://stackoverflow.com/questions/14637979/how-to-permanently-set-path-on-linux
# and also: http://askubuntu.com/questions/391515/changing-etc-environment-did-not-affect-my-environemtn-variables
# and this: http://askubuntu.com/questions/128413/setting-the-path-so-it-applies-to-all-users-including-root-sudo
echo "Updating global profiles to export miniconda bin location to PATH..."
echo "Adding path definition to profiles..."
echo "# Environment varaibles set by Conda init action." | tee -a "${PROFILE_SCRIPT_PATH}" #/etc/*bashrc /etc/profile
echo "export CONDA_BIN_PATH=$CONDA_BIN_PATH" | tee -a "${PROFILE_SCRIPT_PATH}"             #/etc/*bashrc /etc/profile
echo 'export PATH=$CONDA_BIN_PATH:$PATH' | tee -a "${PROFILE_SCRIPT_PATH}"                 #/etc/*bashrc /etc/profile

# 2.3 Update global profiles to add the miniconda location to PATH
echo "Updating global profiles to export miniconda bin location to PATH and set PYTHONHASHSEED ..."
# Fix issue with Python3 hash seed.
# Issue here: https://issues.apache.org/jira/browse/SPARK-13330 (fixed in Spark 2.2.0 release)
# Fix here: http://blog.stuart.axelbrooke.com/python-3-on-spark-return-of-the-pythonhashseed/
echo "Adding PYTHONHASHSEED=0 to profiles and spark-defaults.conf..."
echo "export PYTHONHASHSEED=0" | tee -a "${PROFILE_SCRIPT_PATH}" #/etc/*bashrc  /usr/lib/spark/conf/spark-env.sh
echo "spark.executorEnv.PYTHONHASHSEED=0" >>/etc/spark/conf/spark-defaults.conf

## 3. Ensure that Anaconda Python and PySpark play nice
### http://blog.cloudera.com/blog/2015/09/how-to-prepare-your-apache-hadoop-cluster-for-pyspark-jobs/
echo "Ensure that Anaconda Python and PySpark play nice by all pointing to same Python distro..."
echo "export PYSPARK_PYTHON=$CONDA_BIN_PATH/python" | tee -a "${PROFILE_SCRIPT_PATH}" /etc/environment /usr/lib/spark/conf/spark-env.sh

# CloudSDK libraries are installed in system python
echo 'export CLOUDSDK_PYTHON=/usr/bin/python' | tee -a "${PROFILE_SCRIPT_PATH}" #/etc/*bashrc /etc/profile

echo "Finished bootstrapping via Miniconda, sourcing /etc/profile ..."
source /etc/profile
