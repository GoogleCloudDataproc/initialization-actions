#!/bin/bash

# This script configures Jupyter

set -euxo pipefail

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

readonly JUPYTER_KERNEL_DIR="${INIT_ACTIONS_DIR}/jupyter-sparkmonitor/kernels/pyspark"
readonly KERNEL_GENERATOR="${INIT_ACTIONS_DIR}/jupyter-sparkmonitor/kernels/generate-pyspark.sh"

[[ "${ROLE}" != 'Master' ]] && throw "${0} should only be run on the Master node!"

hadoop fs -mkdir -p "gs://${NOTEBOOK_DIR}"

echo "Installing pyspark Kernel..."
mkdir -p "${JUPYTER_KERNEL_DIR}"
"${KERNEL_GENERATOR}" >"${JUPYTER_KERNEL_DIR}/kernel.json"
/opt/conda/default/bin/jupyter kernelspec install "${JUPYTER_KERNEL_DIR}"
echo "Jupyter setup!"
