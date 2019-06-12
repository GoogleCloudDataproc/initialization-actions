#!/bin/bash

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

readonly DEAFULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO ||
  echo ${DEAFULT_INIT_ACTIONS_REPO})"
readonly LOCAL_INIT_ACTIONS_REPO=/tmp/local-initialization-actions

echo "Cloning initialization actions from '${INIT_ACTIONS_REPO}' repo..."
mkdir "${LOCAL_INIT_ACTIONS_REPO}"
gsutil -m rsync -r "${INIT_ACTIONS_REPO}" "${LOCAL_INIT_ACTIONS_REPO}"
find "${LOCAL_INIT_ACTIONS_REPO}" -name '*.sh' -exec chmod +x {} \;

# Ensure we have GPU drivers installed.
"${LOCAL_INIT_ACTIONS_REPO}/rapids/internal/install-gpu-driver.sh"

# For use with Anaconda component
conda env create --name RAPIDS --file "${LOCAL_INIT_ACTIONS_REPO}/rapids/internal/conda-environment.yml"

# Get Jupyter instance to pickup RAPIDS environment.
if [[ "${ROLE}" == "Master" ]]; then
  /opt/conda/anaconda/bin/conda install -y nb_conda_kernels
  service jupyter restart
fi

"${LOCAL_INIT_ACTIONS_REPO}/rapids/internal/launch-dask.sh"
