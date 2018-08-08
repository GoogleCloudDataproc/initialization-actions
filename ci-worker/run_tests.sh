#!/bin/bash
set -ex

gcloud config set core/disable_prompts 1
gcloud config set component_manager/disable_update_check true
gcloud config set core/disable_usage_reporting true
gcloud config set compute/zone us-west1-c
gcloud config set compute/region us-west1
gcloud config list
gcloud compute config-ssh

#create bucket
export use_internal_ip=true
export bucket=gs://test-$(head /dev/urandom | tr -dc a-z0-9 | head -c 32)
gsutil mb ${bucket}
gsutil lifecycle set bucket_lifecycle.json ${bucket}

#clone repo
git clone https://github.com/${REPO_OWNER}/${REPO_NAME}.git
cd ${REPO_NAME}
git checkout ${PULL_PULL_SHA}

#upload init actions
gsutil -m cp */*sh ${bucket}
gsutil ls ${bucket}

#install pip requirements
cd testing-scripts
pip3 install -r requirements.txt

#invoke tests
python3 -m ${TEST_MODULE} -f


#clean up bucket
gsutil -m rm -r ${bucket}
