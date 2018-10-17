#!/bin/bash

# This script installs Jupyter notebook (http://jupyter.org/) on a Google Cloud
# Dataproc cluster.
# Jupyter is successor of iPython Notebook
#
# This init action depends on init-action for Conda. Git repo and branch for
# init actions might be overridden by specifying INIT_ACTIONS_REPO and
# INIT_ACTIONS_BRANCH metadata keys.

set -exo pipefail

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO \
  || echo 'https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git')"
readonly INIT_ACTIONS_BRANCH="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_BRANCH \
  || echo 'master')"

# Colon-separated list of conda channels to add before installing packages
readonly JUPYTER_CONDA_CHANNELS="$(/usr/share/google/get_metadata_value attributes/JUPYTER_CONDA_CHANNELS)"

# Colon-separated list of conda packages to install, for example 'numpy:pandas'
readonly JUPYTER_CONDA_PACKAGES="$(/usr/share/google/get_metadata_value attributes/JUPYTER_CONDA_PACKAGES)"

echo "Cloning fresh dataproc-initialization-actions from repo ${INIT_ACTIONS_REPO} and branch ${INIT_ACTIONS_BRANCH}..."
git clone -b "${INIT_ACTIONS_BRANCH}" --single-branch "${INIT_ACTIONS_REPO}"

# Ensure we have conda installed.
./dataproc-initialization-actions/conda/bootstrap-conda.sh

source /etc/profile.d/conda.sh

if [ -n "${JUPYTER_CONDA_CHANNELS}" ]; then
  echo "Adding custom conda channels '${JUPYTER_CONDA_CHANNELS//:/ }'"
  conda config --add channels "${JUPYTER_CONDA_CHANNELS//:/,}"
fi

if [ -n "${JUPYTER_CONDA_PACKAGES}" ]; then
  echo "Installing custom conda packages '${JUPYTER_CONDA_PACKAGES/:/ }'"
  # Do not use quotes so that space separated packages turn into multiple arguments
  conda install ${JUPYTER_CONDA_PACKAGES//:/ }
fi

# Install jupyter on all nodes to start with a consistent python environment
# on all nodes. Also, pin the python version to ensure that conda does not
# update python because the latest version of jupyter supports a higher version
# than the one already installed. See issue #300 for more information.
PYTHON="$(ls /opt/conda/bin/python || which python)"
PYTHON_VERSION="$(${PYTHON} --version 2>&1 | cut -d ' ' -f 2)"
conda install jupyter matplotlib "python==${PYTHON_VERSION}"

conda install 'testpath<0.4'

# For storing notebooks on GCS. Pin version to make this script hermetic.
pip install jgscm==0.1.7

if [[ "${ROLE}" == 'Master' ]]; then
    apt-get install -y python-matplotlib libfreetype6-dev pkg-config
    conda install jupyter ipython matplotlib jsonschema jinja2 terminado tornado protobuf pandas seaborn scikit-learn
    conda install -c conda-forge palettable=2.1.1
    if gsutil -q stat "gs://$DATAPROC_BUCKET/notebooks/**"; then
        echo "Pulling notebooks directory to cluster master node..."
        gsutil -m cp -r gs://$DATAPROC_BUCKET/notebooks /root/
    fi
    ./dataproc-initialization-actions/jupyter/internal/setup-jupyter-kernel.sh
    ./dataproc-initialization-actions/jupyter/internal/launch-jupyter-kernel.sh
fi
echo "Completed installing Jupyter!"

# Install Jupyter extensions (if desired)
# TODO: document this in readme
if [[ ! -v "${INSTALL_JUPYTER_EXT}" ]]; then
  INSTALL_JUPYTER_EXT=false
fi
if [[ "${INSTALL_JUPYTER_EXT}" = true ]]; then
  echo "Installing Jupyter Notebook extensions..."
  ./dataproc-initialization-actions/jupyter/internal/bootstrap-jupyter-ext.sh
  echo "Jupyter Notebook extensions installed!"
fi
