#!/bin/bash

# This script configures Jupyter

set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "${DIR}" ]] && DIR="${PWD}"
source "${DIR}/../../util/utils.sh"

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly DATAPROC_BUCKET="$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)"
readonly NOTEBOOK_DIR="${DATAPROC_BUCKET}/notebooks" # Must not include gs:// and must exist
JUPYTER_PORT=$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT || true)
[[ ! $JUPYTER_PORT =~ ^[0-9]+$ ]] && JUPYTER_PORT=8123
readonly JUPYTER_PORT
readonly JUPYTER_AUTH_TOKEN="$(/usr/share/google/get_metadata_value attributes/JUPYTER_AUTH_TOKEN || true)"
readonly JUPYTER_KERNEL_DIR='/dataproc-initialization-actions/jupyter/kernels/pyspark'
readonly KERNEL_GENERATOR='/dataproc-initialization-actions/jupyter/kernels/generate-pyspark.sh'
readonly TOREE_INSTALLER='/dataproc-initialization-actions/jupyter/kernels/install-toree.sh'

[[ "${ROLE}" != 'Master' ]] && throw "${0} should only be run on the Master node!"

hadoop fs -mkdir -p "gs://${NOTEBOOK_DIR}"

echo "Creating Jupyter config..."
jupyter notebook --allow-root --generate-config -y --ip=127.0.0.1
echo "c.Application.log_level = 'DEBUG'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.ip = '0.0.0.0'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.port = ${JUPYTER_PORT}" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.contents_manager_class = 'jgscm.GoogleStorageContentManager'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.GoogleStorageContentManager.default_path = '${NOTEBOOK_DIR}'" >> ~/.jupyter/jupyter_notebook_config.py
echo "c.NotebookApp.token = u'${JUPYTER_AUTH_TOKEN}'" >> ~/.jupyter/jupyter_notebook_config.py

echo "Installing pyspark Kernel..."
chmod 750 "${KERNEL_GENERATOR}"
mkdir -p "${JUPYTER_KERNEL_DIR}"
${KERNEL_GENERATOR} > "${JUPYTER_KERNEL_DIR}/kernel.json"
jupyter kernelspec install "${JUPYTER_KERNEL_DIR}"
echo "c.MappingKernelManager.default_kernel_name = 'pyspark'" >> ~/.jupyter/jupyter_notebook_config.py
${TOREE_INSTALLER}

echo "Jupyter setup!"
