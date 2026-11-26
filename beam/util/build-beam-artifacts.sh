#!/bin/bash

set -exuo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <BEAM_JOB_SERVICE_DESTINATION> <BEAM_CONTAINER_IMAGE_DESTINATION> [<BEAM_SOURCE_VERSION> [<BEAM_SOURCE_DIRECTORY>]]" >&2
  exit 1
fi

readonly BEAM_JOB_SERVICE_DESTINATION="$1"
readonly BEAM_CONTAINER_IMAGE_DESTINATION="$2"
readonly BEAM_SOURCE_VERSION="${3:-master}"

function version_le() { [[ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]]; }
function version_lt() { [[ "$1" = "$2" ]] && return 1 || version_le "$1" "$2"; }

GCLOUD_SDK_VERSION="$(gcloud --version | awk -F'SDK ' '/Google Cloud SDK/ {print $2}')"
GSUTIL="gcloud storage"
if version_lt "${GCLOUD_SDK_VERSION}" "402.0.0"; then
  GSUTIL="gsutil"
fi

function build_job_service() {
  ./gradlew :beam-runners-flink_2.11-job-server:shadowJar
  ${GSUTIL} cp \
    ./runners/flink/job-server/build/libs/beam-runners-flink_2.11-job-server-*-SNAPSHOT.jar \
    ${BEAM_JOB_SERVICE_DESTINATION}/beam-runners-flink_2.11-job-server-${BEAM_SOURCE_VERSION}-SNAPSHOT.jar
}

function build_container() {
  ./gradlew docker
  local images=($(docker images |
    grep '.*-docker-apache' |
    awk '{print $1}'))
  for image in ${images}; do
    local image_destination="${BEAM_CONTAINER_IMAGE_DESTINATION}/$(basename ${image}):${BEAM_SOURCE_VERSION}"
    docker tag $image:latest ${image_destination}
    docker push ${image_destination}
  done
}

function main() {
  if [[ $# -eq 4 ]]; then
    # if there is a 4th argument, use it as the beam source directory
    pushd "$4"
  else
    local workdir=$(mktemp -d)
    pushd ${workdir}
    git clone https://github.com/apache/beam.git
    pushd beam
  fi
  git checkout "${BEAM_SOURCE_VERSION}"
  ./gradlew clean
  build_job_service
  build_container
  popd
}

main "$@"
