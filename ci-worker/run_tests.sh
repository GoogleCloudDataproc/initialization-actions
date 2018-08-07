#!/bin/bash
set -ex

#create bucket
bucket=gs://test-$(head /dev/urandom | tr -dc a-z0-9 | head -c 32)
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
#invoke tests


#clean up bucket
gsutil -m rm -r ${bucket}
