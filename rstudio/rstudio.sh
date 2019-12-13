#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This init script installs RStudio Server on the master node of a Cloud
# Dataproc cluster.

set -euxo pipefail

# Only run on the master node
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

USER_NAME="$(/usr/share/google/get_metadata_value attributes/rstudio-user || echo rstudio)"
USER_PASSWORD="$(/usr/share/google/get_metadata_value attributes/rstudio-password || true)"

RSTUDIO_SERVER_VERSION=1.2.5019
RSTUDIO_SERVER_PACKAGE=rstudio-server-${RSTUDIO_SERVER_VERSION}-amd64.deb

OS_ID=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
OS_CODE=$(lsb_release -cs)

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

# Helper to run any command with Fibonacci backoff.
# If all retries fail, returns last attempt's exit code.
# Args: "$@" is the command to run.
function run_with_retries() {
  local retry_backoff=(1 1 2 3 5 8 13 21 34 55 89 144)
  local -a cmd=("$@")
  echo "About to run '${cmd[*]}' with retries..."

  for ((i = 0; i < ${#retry_backoff[@]}; i++)); do
    if "${cmd[@]}"; then
      return 0
    else
      local sleep_time=${retry_backoff[$i]}
      echo "'${cmd[*]}' attempt $((i + 1)) failed! Sleeping ${sleep_time}." >&2
      sleep "${sleep_time}"
    fi
  done

  echo "Final attempt of '${cmd[*]}'..."
  # Let any final error propagate all the way out to any error traps.
  "${cmd[@]}"
}

if [[ "${ROLE}" == 'Master' ]]; then
  if [[ -n ${USER_PASSWORD} ]] && ((${#USER_PASSWORD} < 7)); then
    echo "You must specify a password of at least 7 characters for user '$USER_NAME' through metadata 'rstudio-password'."
    exit 1
  fi
  if [[ -z "${USER_NAME}" ]]; then
    echo "RStudio user name must not be empty."
    exit 2
  fi
  if [[ "${USER_NAME}" == "${USER_PASSWORD}" ]]; then
    echo "RStudio user name and password must not be the same."
    exit 3
  fi

  # Install RStudio Server
  if [[ "${OS_ID}" == "ubuntu" ]]; then
    REPOSITORY_KEY=E298A3A825C0D65DFD57CBB651716619E084DAB9
  else
    REPOSITORY_KEY=E19F5F87128899B192B1A2C2AD5F960A256A04AF
  fi
  run_with_retries apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys ${REPOSITORY_KEY}
  apt-get install -y software-properties-common
  add-apt-repository "deb http://cran.r-project.org/bin/linux/${OS_ID} ${OS_CODE}-cran35/"
  update_apt_get
  apt-get install -y r-base r-base-dev gdebi-core

  # Download and install RStudio Server package:
  # https://rstudio.com/products/rstudio/download-server/debian-ubuntu/
  if [[ ${OS_CODE} == stretch ]]; then
    RSTUDIO_SERVER_URL=https://download2.rstudio.org/server/debian9/x86_64
  else
    RSTUDIO_SERVER_URL=https://download2.rstudio.org/server/bionic/amd64
  fi
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    ${RSTUDIO_SERVER_URL}/${RSTUDIO_SERVER_PACKAGE} -P /tmp
  gdebi -n /tmp/${RSTUDIO_SERVER_PACKAGE}

  if ! getent group "${USER_NAME}"; then
    groupadd "${USER_NAME}"
  fi
  if ! id -u "${USER_NAME}"; then
    useradd --create-home --gid "${USER_NAME}" "${USER_NAME}"
    if [[ -n "${USER_PASSWORD}" ]]; then
      echo "${USER_NAME}:${USER_PASSWORD}" | chpasswd
    fi
  fi
  if [[ -z "${USER_PASSWORD}" ]]; then
    sed -i 's:ExecStart=\(.*\):Environment=USER=rstudio\nExecStart=\1 --auth-none 1:1' /etc/systemd/system/rstudio-server.service
    systemctl daemon-reload
    systemctl restart rstudio-server
  fi
fi
