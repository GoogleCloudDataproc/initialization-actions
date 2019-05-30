#!/bin/bash

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO \
  || echo 'https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git')"
readonly INIT_ACTIONS_BRANCH="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_BRANCH \
  || echo 'master')"

echo "Cloning dataproc-initialization-actions from repo ${INIT_ACTIONS_REPO} and branch ${INIT_ACTIONS_BRANCH}..."
git clone -b "${INIT_ACTIONS_BRANCH}" --single-branch "${INIT_ACTIONS_REPO}"

./dataproc-initialization-actions/rapids/internal/install-gpu-driver.sh

# For use with Anaconda component
conda env create --name RAPIDS --file dataproc-initialization-actions/rapids/internal/conda-environment.yml

# For use with miniconda and jupyter init-actions instead of Anaconda component
# bash rapids/internal/install-rapids.sh

# Get Jupyter instance to pickup RAPIDS environment.
# TODO: Detect if Jupyter optional component is configured.
if [[ "${ROLE}" == "Master" ]]; then
    /opt/conda/anaconda/bin/conda install -y nb_conda_kernels
    service jupyter restart
fi

./dataproc-initialization-actions/rapids/internal/launch-dask.sh
