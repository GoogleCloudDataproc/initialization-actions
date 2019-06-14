#!/bin/bash

set -euxo pipefail

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

readonly DEAFULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO ||
  echo ${DEAFULT_INIT_ACTIONS_REPO})"

echo "Cloning RAPIDS initialization action from '${INIT_ACTIONS_REPO}' ..."
RAPIDS_INIT_ACTION_DIR=$(mktemp -d -t rapids-init-action-XXXX)
readonly RAPIDS_INIT_ACTION_DIR
gsutil -m rsync -r "${INIT_ACTIONS_REPO}/rapids" "${RAPIDS_INIT_ACTION_DIR}"
find "${RAPIDS_INIT_ACTION_DIR}" -name '*.sh' -exec chmod +x {} \;

# Ensure we have GPU drivers installed.
"${RAPIDS_INIT_ACTION_DIR}/internal/install-gpu-driver.sh"

# For use with Anaconda component
conda env create --name RAPIDS --file "${RAPIDS_INIT_ACTION_DIR}/internal/conda-environment.yml"

# Get Jupyter instance to pickup RAPIDS environment.
if [[ "${ROLE}" == "Master" ]]; then
  /opt/conda/anaconda/bin/conda install -y nb_conda_kernels
  service jupyter restart || true
fi

"${RAPIDS_INIT_ACTION_DIR}/internal/launch-dask.sh"
