#!/usr/bin/env bash

apt install libopenblas-base libomp-dev

readonly DATAPROC_BUCKET="$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)"
readonly CONDA_ENV_YAML_GSC_LOC="gs://dataproc-initialization-actions/rapids/conda-environment.yml"
readonly CONDA_ENV_YAML_PATH="/root/conda-environment.yml"

echo "Downloading conda environment at $CONDA_ENV_YAML_GSC_LOC to $CONDA_ENV_YAML_PATH... "
gsutil -m cp -r $CONDA_ENV_YAML_GSC_LOC $CONDA_ENV_YAML_PATH
gsutil -m cp -r gs://dataproc-initialization-actions/conda/bootstrap-conda.sh .
gsutil -m cp -r gs://dataproc-initialization-actions/conda/install-conda-env.sh .

chmod 755 ./*conda*.sh

# Install Miniconda/Conda
./bootstrap-conda.sh
# Create/Update Conda environment via Conda yaml
CONDA_ENV_YAML=$CONDA_ENV_YAML_PATH ./install-conda-env.sh

source /etc/profile.d/conda.sh
