#!/bin/bash

set -euxo pipefail

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

readonly DEAFULT_RAPIDS_INIT_ACTION_GCS_DIR=gs://dataproc-initialization-actions/rapids
readonly RAPIDS_INIT_ACTION_GCS_DIR="$(/usr/share/google/get_metadata_value attributes/RAPIDS_INIT_ACTION_DIR ||
  echo ${DEAFULT_RAPIDS_INIT_ACTION_GCS_DIR})"

RAPIDS_INIT_ACTION_DIR=$(mktemp -d -t rapid-init-action-XXXX)
readonly RAPIDS_INIT_ACTION_DIR
export RAPIDS_INIT_ACTION_DIR

echo "Cloning RAPIDS initialization action from '${RAPIDS_INIT_ACTION_GCS_DIR}' ..."
gsutil -m rsync -r "${RAPIDS_INIT_ACTION_GCS_DIR}/rapids" "${RAPIDS_INIT_ACTION_DIR}"
find "${RAPIDS_INIT_ACTION_DIR}" -name '*.sh' -exec chmod +x {} \;

# Ensure we have GPU drivers installed.
"${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver.sh"

# For use with Anaconda component
conda env create --name RAPIDS --file "${RAPIDS_INIT_ACTION_DIR}/internal/conda-environment.yml"

# Get Jupyter instance to pickup RAPIDS environment.
if [[ "${ROLE}" == "Master" ]]; then
  /opt/conda/anaconda/bin/conda install -y nb_conda_kernels
  service jupyter restart
fi

"${RAPIDS_INIT_ACTION_DIR}/internal/launch-dask.sh"
