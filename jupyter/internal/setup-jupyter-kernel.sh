#!/bin/bash
set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "$DIR" ]] && DIR="$PWD"
source "$DIR/../../util/utils.sh"

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
JUPYTER_NOTEBOOK_DIR="/root/notebooks"
JUPYTER_PORT=$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT || true)
[[ ! $JUPYTER_PORT =~ ^[0-9]+$ ]] && JUPYTER_PORT=8123
JUPYTER_IP=*

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
JUPYTER_KERNEL_DIR='/dataproc-initialization-actions/jupyter/kernels/pyspark'
KERNEL_GENERATOR='/dataproc-initialization-actions/jupyter/kernels/generate_pyspark.sh'
chmod 750 ${KERNEL_GENERATOR}
mkdir -p ${JUPYTER_KERNEL_DIR}
${KERNEL_GENERATOR} > "${JUPYTER_KERNEL_DIR}/kernel.json"
jupyter kernelspec install $JUPYTER_KERNEL_DIR
echo "c.MappingKernelManager.default_kernel_name = 'pyspark'" >> ~/.jupyter/jupyter_notebook_config.py

echo "Jupyter setup!"

