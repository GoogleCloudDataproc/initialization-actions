#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euxo pipefail

readonly LIVY_VERSION=$(/usr/share/google/get_metadata_value attributes/livy-version || echo 0.7.0)
readonly LIVY_PKG_NAME=apache-livy-${LIVY_VERSION}-incubating-bin
readonly LIVY_URL=https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}-incubating/${LIVY_PKG_NAME}.zip

readonly LIVY_DIR=/usr/local/lib/livy
readonly LIVY_BIN=${LIVY_DIR}/bin
readonly LIVY_CONF=${LIVY_DIR}/conf

# Generate livy configuration file.
function make_livy_conf() {
  cat <<EOF >"${LIVY_CONF}/livy.conf"
livy.spark.master = $(grep spark.master /etc/spark/conf/spark-defaults.conf | cut -d= -f2)
livy.spark.deploy-mode = $(grep spark.submit.deployMode /etc/spark/conf/spark-defaults.conf | cut -d= -f2)
EOF
}

# Generate livy environment file.
function make_livy_env() {
  cat <<EOF >"${LIVY_CONF}/livy-env.sh"
export SPARK_HOME=/usr/lib/spark
export SPARK_CONF_DIR=/etc/spark/conf
export HADOOP_CONF_DIR=/etc/hadoop/conf
export LIVY_LOG_DIR=/var/log/livy
EOF
}

# Create Livy service.
function create_systemd_unit() {
  cat <<EOF >"/etc/systemd/system/livy.service"
[Unit]
Description=Apache Livy service
After=network.target

[Service]
Group=livy
User=livy
Type=forking
ExecStart=${LIVY_BIN}/livy-server start
ExecStop=${LIVY_BIN}/livy-server stop
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
}

function main() {
  # Only run this initialization action on the master node.
  local role
  role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  if [[ ${role} != Master ]]; then
    exit 0
  fi

  # Download Livy binary.
  local temp
  temp=$(mktemp -d -t livy-init-action-XXXX)

  wget -nv --timeout=30 --tries=5 --retry-connrefused "${LIVY_URL}" -P "${temp}"

  unzip -q "${temp}/${LIVY_PKG_NAME}.zip" -d /usr/local/lib/
  ln -s "/usr/local/lib/${LIVY_PKG_NAME}" "${LIVY_DIR}"

  # Create Livy user.
  useradd -G hadoop livy

  # Setup livy package.
  chown -R -L livy:livy "${LIVY_DIR}"

  # Generate livy configuration file.
  make_livy_conf

  # Setup log directory.
  mkdir /var/log/livy
  chown -R livy:livy /var/log/livy

  # Cleanup temp files.
  rm -Rf "${temp}"

  # Generate livy environment file.
  make_livy_env

  # Start livy service.
  create_systemd_unit
  systemctl enable livy.service
  systemctl start livy.service
}

main
