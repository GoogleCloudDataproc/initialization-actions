#!/bin/bash

# Run from root directory of initialization-actions checkout

IMAGE="rapids-actions-image:$BUILD_ID"
max_parallel_tests=10

IMAGE_VERSION="$1"
shift
if [[ -z "${IMAGE_VERSION}" ]] ; then
       IMAGE_VERSION="$(jq -r .IMAGE_VERSION        env.json)" ; fi ; export IMAGE_VERSION

#declare -a TESTS_TO_RUN=('dask:test_dask' 'rapids:test_rapids')
#declare -a TESTS_TO_RUN=('dask:test_dask')
#declare -a TESTS_TO_RUN=('rapids:test_rapids')
if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]] && [[ -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  echo "Authenticating gcloud with service account key..."
  gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}"
  gcloud config set project "${PROJECT_ID}"
fi

declare -a TESTS_TO_RUN=('gpu:test_gpu')

time bazel test \
  --jobs="${max_parallel_tests}" \
  --local_test_jobs="${max_parallel_tests}" \
  --action_env="INTERNAL_IP_SSH=true" \
  --test_env="PROJECT_ID=${PROJECT_ID}" \
  --test_env="REGION=${REGION}" \
  --test_env="GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}" \
  --test_output="errors" \
  --test_arg="--image_version=${IMAGE_VERSION}" \
  "$@" \
  "${TESTS_TO_RUN[@]}"
