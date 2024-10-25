#!/bin/bash

# Run from root directory of initialization-actions checkout

IMAGE="rapids-actions-image:$BUILD_ID"
max_parallel_tests=10

IMAGE_VERSION="$1"
if [[ -z "${IMAGE_VERSION}" ]] ; then
       IMAGE_VERSION="$(jq -r .IMAGE_VERSION        env.json)" ; fi ; export IMAGE_VERSION

#declare -a TESTS_TO_RUN=('dask:test_dask' 'rapids:test_rapids')
#declare -a TESTS_TO_RUN=('dask:test_dask')
declare -a TESTS_TO_RUN=('rapids:test_rapids')

time bazel test \
  --jobs="${max_parallel_tests}" \
  --local_test_jobs="${max_parallel_tests}" \
  --flaky_test_attempts=3 \
  --action_env="INTERNAL_IP_SSH=true" \
  --test_output="errors" \
  --test_arg="--image_version=${IMAGE_VERSION}" \
  "${TESTS_TO_RUN[@]}"
