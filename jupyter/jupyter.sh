#!/usr/bin/env bash
set -e

ROLE=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/dataproc-role)
INIT_ACTIONS_REPO=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/INIT_ACTIONS_REPO || true)
INIT_ACTIONS_REPO="${INIT_ACTIONS_REPO:-https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git}"
INIT_ACTIONS_BRANCH=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/INIT_ACTIONS_BRANCH || true)
INIT_ACTIONS_BRANCH="${INIT_ACTIONS_BRANCH:-master}"

# Colon-separated list of conda channels to add before installing packages
JUPYTER_CONDA_CHANNELS=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/JUPYTER_CONDA_CHANNELS || true)
# Colon-separated list of conda packages to install, for example 'numpy:pandas'
JUPYTER_CONDA_PACKAGES=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/JUPYTER_CONDA_PACKAGES || true)

echo "Cloning fresh dataproc-initialization-actions from repo $INIT_ACTIONS_REPO and branch $INIT_ACTIONS_BRANCH..."
git clone -b "$INIT_ACTIONS_BRANCH" --single-branch $INIT_ACTIONS_REPO
# Ensure we have conda installed.
./dataproc-initialization-actions/conda/bootstrap-conda.sh

source /etc/profile.d/conda.sh

if [ -n "${JUPYTER_CONDA_CHANNELS}" ]; then
  echo "Adding custom conda channels '$(echo ${JUPYTER_CONDA_CHANNELS} | tr ':' ' ')'"
  conda config --add channels $(echo ${JUPYTER_CONDA_CHANNELS} | tr ':' ',')
fi

if [ -n "${JUPYTER_CONDA_PACKAGES}" ]; then
  echo "Installing custom conda packages '$(echo ${JUPYTER_CONDA_PACKAGES} | tr ':' ' ')'"
  conda install $(echo ${JUPYTER_CONDA_PACKAGES} | tr ':' ' ')
fi

if [[ "${ROLE}" == 'Master' ]]; then
    conda install jupyter

    # For storing notebooks on GCS. Pin version to make this script hermetic.
    pip install jgscm==0.1.7

    ./dataproc-initialization-actions/jupyter/internal/setup-jupyter-kernel.sh
    ./dataproc-initialization-actions/jupyter/internal/launch-jupyter-kernel.sh
fi
echo "Completed installing Jupyter!"

# Install Jupyter extensions (if desired)
# TODO: document this in readme
if [[ ! -v $INSTALL_JUPYTER_EXT ]]
    then
    INSTALL_JUPYTER_EXT=false
fi
if [[ "$INSTALL_JUPYTER_EXT" = true ]]
then
    echo "Installing Jupyter Notebook extensions..."
    ./dataproc-initialization-actions/jupyter/internal/bootstrap-jupyter-ext.sh
    echo "Jupyter Notebook extensions installed!"
fi

