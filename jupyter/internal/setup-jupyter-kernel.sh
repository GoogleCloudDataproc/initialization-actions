#!/bin/bash
set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "$DIR" ]] && DIR="$PWD"
source "$DIR/../../util/utils.sh"

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
DATAPROC_VERSION=$(gcloud dataproc clusters describe $CLUSTER_NAME | grep imageVersion | tr -d "'" | awk '{print $2}')
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
JUPYTER_NOTEBOOK_DIR="/root/notebooks"
JUPYTER_PORT=$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT || true)
[[ ! $JUPYTER_PORT =~ ^[0-9]+$ ]] && JUPYTER_PORT=8123
JUPYTER_IP=*


if [[ "${DATAPROC_VERSION}" = "1.1" ]]; then
  JUPYTER_KERNEL_DIR="/dataproc-initialization-actions/jupyter/dataproc-1-1/kernels/pyspark"
else
  JUPYTER_KERNEL_DIR="/dataproc-initialization-actions/jupyter/kernels/pyspark"
fi
readonly JUPYTER_KERNEL_DIR

[[ "${ROLE}" != 'Master' ]] && throw "$0 should only be run on the Master node!"

echo "Creating notebook directory $JUPYTER_NOTEBOOK_DIR if necessary..."
[[ ! -d $JUPYTER_NOTEBOOK_DIR ]] && mkdir -p $JUPYTER_NOTEBOOK_DIR

echo "Creating Jupyter config..."
jupyter notebook --generate-config -y --ip=127.0.0.1
echo "c.Application.log_level = 'DEBUG'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '*'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = $JUPYTER_PORT" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.notebook_dir = '$JUPYTER_NOTEBOOK_DIR'" >> ~/.jupyter/jupyter_notebook_config.py

echo "Installing pyspark Kernel..."
jupyter kernelspec install $JUPYTER_KERNEL_DIR
echo "c.MappingKernelManager.default_kernel_name = 'pyspark'" >> ~/.jupyter/jupyter_notebook_config.py

echo "Jupyter setup!"

