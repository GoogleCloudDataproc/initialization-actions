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

readonly DATAPROC_VERSION="$(grep DATAPROC_VERSION /etc/environment | cut -d= -f2 | sed -e 's/"//g')"

if [[ "${DATAPROC_VERSION}" == '1.0' ]] || [[ "${DATAPROC_VERSION}" == '1.1' ]]; then
  readonly LIVY_VERSION="0.5.0"
  readonly LIVY_PKG_NAME="livy-${LIVY_VERSION}-incubating-bin"
else
  readonly LIVY_VERSION="0.6.0"
  readonly LIVY_PKG_NAME="apache-livy-${LIVY_VERSION}-incubating-bin"
fi

readonly LIVY_DIR="/usr/local/lib/livy"
readonly LIVY_BIN="${LIVY_DIR}/bin"
readonly LIVY_CONF="${LIVY_DIR}/conf"

# Apache mirror redirector.
readonly APACHE_MIRROR="https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename"
readonly PKG_PATH="incubator/livy/${LIVY_VERSION}-incubating/${LIVY_PKG_NAME}.zip"

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
  if [[ "${role}" != 'Master' ]]; then
    exit 0
  fi

  # Download Livy binary.
  local temp
  temp=$(mktemp -d)
  wget --progress=dot:mega --timeout=30 -O "${temp}/livy.zip" "${APACHE_MIRROR}=${PKG_PATH}"
  unzip -q "${temp}/livy.zip" -d "${temp}"

  # Create Livy user.
  useradd -G hadoop livy

  # Setup livy package.
  install -d "${LIVY_DIR}"
  cp -r "${temp}/${LIVY_PKG_NAME}/"* "${LIVY_DIR}"
  chown -R "livy:livy" "${LIVY_DIR}"

  # Setup log directory.
  mkdir /var/log/livy
  chown -R "livy:livy" /var/log/livy

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
