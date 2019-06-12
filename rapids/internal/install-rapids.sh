#!/usr/bin/env bash

readonly LOCAL_INIT_ACTIONS_REPO=/tmp/local-initialization-actions

readonly CONDA_ENV_YAML_REPO_PATH="${LOCAL_INIT_ACTIONS_REPO}/rapids/conda-environment.yml"
readonly CONDA_ENV_YAML_PATH="/root/conda-environment.yml"

apt install libopenblas-base libomp-dev

echo "Copying Conda environment from '${CONDA_ENV_YAML_REPO_PATH}' to '${CONDA_ENV_YAML_PATH}' ... "
cp "${CONDA_ENV_YAML_REPO_PATH}" "${CONDA_ENV_YAML_PATH}"

# Install Miniconda/Conda
bash "${LOCAL_INIT_ACTIONS_REPO}/conda/bootstrap-conda.sh"
# Create/Update Conda environment via Conda yaml
CONDA_ENV_YAML=$CONDA_ENV_YAML_PATH bash "${LOCAL_INIT_ACTIONS_REPO}/conda/install-conda-env.sh"

source /etc/profile.d/conda.sh
