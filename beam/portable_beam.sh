#!/usr/bin/env bash

set -euxo pipefail

readonly LOCAL_JAR_NAME='beam-runners-flink_2.11-job-server.jar'
readonly RELEASE_SNAPSHOT_URL="http://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink_2.11-job-server/2.6.0/beam-runners-flink_2.11-job-server-2.6.0.jar"
readonly SERVICE_INSTALL_DIR='/usr/lib/beam-job-service'
readonly SERVICE_WORKING_DIR='/var/lib/beam-job-service'
readonly SERVICE_WORKING_USER='yarn'

function is_master() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ "$role" == 'Master' ]] ; then
    true
  else
    false
  fi
}

function install_job_service() {
  local master_url="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
  mkdir -p "${SERVICE_INSTALL_DIR}"
  pushd "${SERVICE_INSTALL_DIR}"
  curl -o "${LOCAL_JAR_NAME}" "${RELEASE_SNAPSHOT_URL}"
  popd
  mkdir -p "${SERVICE_WORKING_DIR}"
  chown -R "${SERVICE_WORKING_USER}" "${SERVICE_WORKING_DIR}"
  cat > "/etc/systemd/system/beam-job-service.service" <<EOF
[Unit]
Description=Beam Job Service
After=default.target

[Service]
Type=simple
User=${SERVICE_WORKING_USER}
WorkingDirectory=${SERVICE_WORKING_DIR}
ExecStart=/usr/bin/java -jar ${SERVICE_INSTALL_DIR}/${LOCAL_JAR_NAME} --job-host=${master_url}:8099 --artifacts-dir=hdfs:///tmp/beam-artifacts --flink-master-url=${master_url}:8081
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
systemctl enable beam-job-service
}

function run_job_service() {
  systemctl restart beam-job-service
}

function main() {
  if [[ is_master ]]; then
    install_job_service
    run_job_service
  fi
}

main "$@"
