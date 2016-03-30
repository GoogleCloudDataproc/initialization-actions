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
./dataproc-initialization-actions/conda/bootstrap-conda.sh
./dataproc-initialization-actions/conda/install-conda-env.sh
source /etc/profile.d/conda_config.sh
if [[ "${ROLE}" == 'Master' ]]; then
    if gsutil -q stat gs://$DATAPROC_BUCKET/notebooks/; then
        echo "Pulling notebooks directory to cluster master node..."
        gsutil -m cp -r gs://$DATAPROC_BUCKET/notebooks /root/
    fi

    ./dataproc-initialization-actions/jupyter/internal/setup-jupyter-kernel.sh
    #./dataproc-initialization-actions/jupyter/internal/bootstrap-jupyter-ext.sh
    ./dataproc-initialization-actions/jupyter/internal/launch-jupyter-kernel.sh
fi
echo "Completed bootstrapping dataproc cluster!"

