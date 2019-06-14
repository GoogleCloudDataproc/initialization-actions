#!/usr/bin/env bash

set -euxo pipefail

readonly DEAFULT_CONDA_INIT_ACTION_GCS_DIR=gs://dataproc-initialization-actions/conda
readonly CONDA_INIT_ACTION_GCS_DIR="$(/usr/share/google/get_metadata_value attributes/CONDA_INIT_ACTION_DIR ||
  echo ${DEAFULT_CONDA_INIT_ACTION_GCS_DIR})"

readonly CONDA_ENV_YAML_REPO_PATH="${RAPIDS_INIT_ACTION_DIR}/conda-environment.yml"
readonly CONDA_ENV_YAML_PATH="/root/conda-environment.yml"

apt install -y libopenblas-base libomp-dev

echo "Copying Conda environment from '${CONDA_ENV_YAML_REPO_PATH}' to '${CONDA_ENV_YAML_PATH}' ... "
cp "${CONDA_ENV_YAML_REPO_PATH}" "${CONDA_ENV_YAML_PATH}"

# Install Miniconda/Conda
echo "Cloning Conda initialization action from '${CONDA_INIT_ACTION_GCS_DIR}' ..."
CONDA_INIT_ACTION_DIR=$(mktemp -d -t conda-init-action-XXXX)
readonly CONDA_INIT_ACTION_DIR
gsutil -m rsync -r "${CONDA_INIT_ACTION_GCS_DIR}" "${CONDA_INIT_ACTION_DIR}"
find "${CONDA_INIT_ACTION_DIR}" -name '*.sh' -exec chmod +x {} \;

"${CONDA_INIT_ACTION_DIR}/bootstrap-conda.sh"
# Create/Update Conda environment via Conda yaml
CONDA_ENV_YAML="${CONDA_ENV_YAML_PATH}" "${CONDA_INIT_ACTION_DIR}/install-conda-env.sh"

source /etc/profile.d/conda.sh
