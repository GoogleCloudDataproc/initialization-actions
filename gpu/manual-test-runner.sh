#!/bin/bash

# This script sets up the gcloud environment and launches tests in a screen session
#
# To run the script, the following will bootstrap
#
# git clone git@github.com:LLC-Technologies-Collier/initialization-actions
# git checkout gpu-20241121
# cd initialization-actions
# cp gpu/env.json.sample env.json
# vi env.json
# docker build -f gpu/Dockerfile -t gpu-init-actions-runner:latest .
# time docker run -it gpu-init-actions-runner:latest gpu/manual-test-runner.sh
#
# The bazel run(s) happen in separate screen windows.
#  To see a list of screen windows, press ^a "
# Num Name
#
#   0 monitor
#   1 2.0-debian10
#   2 sh


readonly timestamp="$(date +%F-%H-%M)"
export BUILD_ID="$(uuidgen)"

tmp_dir="/tmp/${BUILD_ID}"
log_dir="${tmp_dir}/logs"
mkdir -p "${log_dir}"

IMAGE_VERSION="$1"
if [[ -z "${IMAGE_VERSION}" ]] ; then
       IMAGE_VERSION="$(jq -r .IMAGE_VERSION        env.json)" ; fi ; export IMAGE_VERSION
export PROJECT_ID="$(jq    -r .PROJECT_ID           env.json)"
export REGION="$(jq        -r .REGION               env.json)"
export BUCKET="$(jq        -r .BUCKET               env.json)"

gcs_log_dir="gs://${BUCKET}/${BUILD_ID}/logs"

function exit_handler() {
  RED='\\e[0;31m'
  GREEN='\\e[0;32m'
  NC='\\e[0m'
  echo 'Cleaning up before exiting.'

  # TODO: list clusters which match our BUILD_ID and clean them up
  # TODO: remove any test related resources in the project

  echo 'Uploading local logs to GCS bucket.'
  gsutil -m rsync -r "${log_dir}/" "${gcs_log_dir}/"

  if [[ -f "${tmp_dir}/tests_success" ]]; then
    echo -e "${GREEN}Workflow succeeded${NC}, check logs at ${log_dir}/ or ${gcs_log_dir}/"
    exit 0
  else
    echo -e "${RED}Workflow failed${NC}, check logs at ${log_dir}/ or ${gcs_log_dir}/"
    exit 1
  fi
}

trap exit_handler EXIT

# screen session name
session_name="manual-gpu-tests"

gcloud config set project ${PROJECT_ID}
gcloud config set dataproc/region ${REGION}
gcloud auth login
gcloud config set compute/region ${REGION}

export INTERNAL_IP_SSH="true"

# Run tests in screen session so we can monitor the container in another window
screen -US "${session_name}" -c gpu/bazel.screenrc



