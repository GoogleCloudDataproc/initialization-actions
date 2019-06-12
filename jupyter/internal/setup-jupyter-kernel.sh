#!/bin/bash

# This script configures Jupyter

set -euo pipefail

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

readonly LOCAL_INIT_ACTIONS_REPO=/tmp/local-initialization-actions
readonly JUPYTER_KERNEL_DIR="${LOCAL_INIT_ACTIONS_REPO}/jupyter/kernels/pyspark"
readonly KERNEL_GENERATOR="${LOCAL_INIT_ACTIONS_REPO}/jupyter/kernels/generate-pyspark.sh"
readonly TOREE_INSTALLER="${LOCAL_INIT_ACTIONS_REPO}/jupyter/kernels/install-toree.sh"

[[ "${ROLE}" != 'Master' ]] && throw "${0} should only be run on the Master node!"

hadoop fs -mkdir -p "gs://${NOTEBOOK_DIR}"

echo "Creating Jupyter config..."
jupyter notebook --allow-root --generate-config -y --ip=127.0.0.1

cat <<EOF >>~/.jupyter/jupyter_notebook_config.py
c.Application.log_level = 'DEBUG'
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.open_browser = False
c.NotebookApp.port = ${JUPYTER_PORT}
c.NotebookApp.contents_manager_class = 'jgscm.GoogleStorageContentManager'
c.GoogleStorageContentManager.default_path = '${NOTEBOOK_DIR}'
c.NotebookApp.token = u'${JUPYTER_AUTH_TOKEN}'
EOF

echo "Installing pyspark Kernel..."
mkdir -p "${JUPYTER_KERNEL_DIR}"
bash "${KERNEL_GENERATOR}" >"${JUPYTER_KERNEL_DIR}/kernel.json"
jupyter kernelspec install "${JUPYTER_KERNEL_DIR}"
echo "c.MappingKernelManager.default_kernel_name = 'pyspark'" >>~/.jupyter/jupyter_notebook_config.py
bash "${TOREE_INSTALLER}"

echo "Jupyter setup!"
