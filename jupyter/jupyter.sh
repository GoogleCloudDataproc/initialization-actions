#!/usr/bin/env bash
set -e

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
INIT_ACTIONS_REPO=$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO || true)
INIT_ACTIONS_REPO="${INIT_ACTIONS_REPO:-https://github.com/GoogleCloudPlatform/dataproc-initialization-actions.git}"
INIT_ACTIONS_BRANCH=$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_BRANCH || true)
INIT_ACTIONS_BRANCH="${INIT_ACTIONS_BRANCH:-master}"
DATAPROC_BUCKET=$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)

echo "Cloning fresh dataproc-initialization-actions from repo $INIT_ACTIONS_REPO and branch $INIT_ACTIONS_BRANCH..."
git clone -b "$INIT_ACTIONS_BRANCH" --single-branch $INIT_ACTIONS_REPO
# Ensure we have conda installed.
./dataproc-initialization-actions/conda/bootstrap-conda.sh
#./dataproc-initialization-actions/conda/install-conda-env.sh

source /etc/profile.d/conda_config.sh
if [[ "${ROLE}" == 'Master' ]]; then
    apt-get install -y python-matplotlib libfreetype6-dev pkg-config
    conda install jupyter ipython matplotlib jsonschema jinja2 terminado tornado protobuf pandas seaborn scikit-learn
    pip install -U palettable pylab 
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

