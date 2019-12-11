#!/usr/bin/env bash

set -euxo pipefail

readonly LOCAL_JAR_NAME='beam-runners-flink_2.11-job-server.jar'
readonly SERVICE_INSTALL_DIR='/usr/lib/beam-job-service'
readonly SERVICE_WORKING_DIR='/var/lib/beam-job-service'
readonly SERVICE_WORKING_USER='yarn'

readonly ARTIFACTS_GCS_PATH_METADATA_KEY='beam-artifacts-gcs-path'
readonly RELEASE_SNAPSHOT_URL_METADATA_KEY="beam-job-service-snapshot"
readonly RELEASE_SNAPSHOT_URL_DEFAULT="http://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink_2.11-job-server/2.6.0/beam-runners-flink_2.11-job-server-2.6.0.jar"

readonly BEAM_IMAGE_ENABLE_PULL_METADATA_KEY="beam-image-enable-pull"
readonly BEAM_IMAGE_ENABLE_PULL_DEFAULT=false
readonly BEAM_IMAGE_VERSION_METADATA_KEY="beam-image-version"
readonly BEAM_IMAGE_VERSION_DEFAULT="master"
readonly BEAM_IMAGE_REPOSITORY_KEY="beam-image-repository"
readonly BEAM_IMAGE_REPOSITORY_DEFAULT="apache.bintray.io/beam"

readonly START_FLINK_YARN_SESSION_METADATA_KEY='flink-start-yarn-session'
# Set this to true to start a flink yarn session at initialization time.
readonly START_FLINK_YARN_SESSION_DEFAULT=true

function is_master() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ "$role" == 'Master' ]]; then
    true
  else
    false
  fi
}

function get_artifacts_dir() {
  /usr/share/google/get_metadata_value "attributes/${ARTIFACTS_GCS_PATH_METADATA_KEY}" ||
    echo "gs://$(/usr/share/google/get_metadata_value "attributes/dataproc-bucket")/beam-artifacts"
}

function download_snapshot() {
  readonly snapshot_url="${1}"
  readonly protocol="$(echo "${snapshot_url}" | head -c5)"
  if [ "${protocol}" = "gs://" ]; then
    gsutil cp "${snapshot_url}" "${LOCAL_JAR_NAME}"
  else
    curl -o "${LOCAL_JAR_NAME}" "${snapshot_url}"
  fi
}

function flink_master_url() {
  local start_flink_yarn_session="$(/usr/share/google/get_metadata_value \
    "attributes/${START_FLINK_YARN_SESSION_METADATA_KEY}" ||
    echo "${START_FLINK_YARN_SESSION_DEFAULT}")"
  # TODO: delete this workaround when the beam job service is able to understand
  # flink in yarn mode.
  if ${start_flink_yarn_session}; then
    # grab final field from the first yarn application that contains 'flink'
    yarn application -list |
      grep -i 'flink' |
      head -n1 |
      awk -F $'\t' '{print $9}' |
      cut -c8-
  else
    echo "localhost:8081"
  fi
}

function install_job_service() {
  local master_url="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
  local artifacts_dir="$(get_artifacts_dir)"
  local release_snapshot_url="$(/usr/share/google/get_metadata_value \
    "attributes/${RELEASE_SNAPSHOT_URL_METADATA_KEY}" ||
    echo "${RELEASE_SNAPSHOT_URL_DEFAULT}")"

  echo "Retrieving Beam Job Service snapshot from ${release_snapshot_url}"

  local flink_master="$(flink_master_url)"
  echo "Resolved flink master to: '${master_url}'"

  mkdir -p "${SERVICE_INSTALL_DIR}"
  pushd "${SERVICE_INSTALL_DIR}"
  download_snapshot "${release_snapshot_url}"
  popd
  mkdir -p "${SERVICE_WORKING_DIR}"
  chown -R "${SERVICE_WORKING_USER}" "${SERVICE_WORKING_DIR}"

  cat >"/etc/systemd/system/beam-job-service.service" <<EOF
[Unit]
Description=Beam Job Service
After=default.target

[Service]
Type=simple
User=${SERVICE_WORKING_USER}
WorkingDirectory=${SERVICE_WORKING_DIR}
ExecStart=/usr/bin/java \
  -jar ${SERVICE_INSTALL_DIR}/${LOCAL_JAR_NAME} \
  --job-host=${master_url}\
  --artifacts-dir=${artifacts_dir} \
  --flink-master-url=${flink_master}
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
  systemctl enable beam-job-service
}

function run_job_service() {
  systemctl restart beam-job-service
}

function pull_beam_images() {
  local beam_image_version="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_IMAGE_VERSION_METADATA_KEY}" ||
    echo "${BEAM_IMAGE_VERSION_DEFAULT}")"
  local image_repo="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_IMAGE_REPOSITORY_KEY}" ||
    echo "${BEAM_IMAGE_REPOSITORY_DEFAULT}")"
  # Pull beam images with `sudo -i` since if pulling from GCR, yarn will be
  # configured with GCR authorization
  sudo -u yarn -i docker pull "${image_repo}/go:${beam_image_version}"
  sudo -u yarn -i docker pull "${image_repo}/python:${beam_image_version}"
  sudo -u yarn -i docker pull "${image_repo}/java:${beam_image_version}"
}

function main() {
  if [[ is_master ]]; then
    install_job_service
    run_job_service
  fi

  local pull_images="$(/usr/share/google/get_metadata_value \
    "attributes/${BEAM_IMAGE_ENABLE_PULL_METADATA_KEY}" ||
    echo "${BEAM_IMAGE_ENABLE_PULL_DEFAULT}")"
  if ${pull_images}; then
    pull_beam_images
  fi
}

main "$@"
