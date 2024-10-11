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

OS_ID=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
OS_CODE=$(lsb_release -cs)

RSTUDIO_SERVER_VERSION=''
if [[ "${OS_ID}" == debian ]] || [[ "${OS_ID}" == ubuntu ]]; then
  RSTUDIO_SERVER_VERSION=1.2.5019
  if [[ "${OS_CODE}" == stretch ]]; then
    RSTUDIO_SERVER_URL=https://download2.rstudio.org/server/debian9/x86_64
  elif [[ "$OS_CODE" == bookworm ]] || [[ "${OS_CODE}" == jammy ]]; then
  # Choose Ubuntu 22 compatible version, see
  # https://community.rstudio.com/t/dependency-error-when-installing-rstudio-on-ubuntu-22-04-with-libssl/135397.
    RSTUDIO_SERVER_VERSION=2023.09.1-494
    RSTUDIO_SERVER_URL=https://download2.rstudio.org/server/jammy/amd64
  else
    RSTUDIO_SERVER_URL=https://download2.rstudio.org/server/bionic/amd64
  fi
else
  echo "Error: OS ${OS_ID} is not supported"
  exit 1
fi

RSTUDIO_SERVER_PACKAGE=rstudio-server-${RSTUDIO_SERVER_VERSION}-amd64.deb
RSTUDIO_SERVER_PACKAGE_URI=${RSTUDIO_SERVER_URL}/${RSTUDIO_SERVER_PACKAGE}

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

function remove_old_backports {
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  oldstable=$(curl -s https://deb.debian.org/debian/dists/oldstable/Release | awk '/^Codename/ {print $2}');
  stable=$(curl -s https://deb.debian.org/debian/dists/stable/Release | awk '/^Codename/ {print $2}');

  matched_files="$(grep -rsil '\-backports' /etc/apt/sources.list*)"
  if [[ -n "$matched_files" ]]; then
    for filename in "$matched_files"; do
      grep -e "$oldstable-backports" -e "$stable-backports" "$filename" || \
        sed -i -e 's/^.*-backports.*$//' "$filename"
    done
  fi
}

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

function install_package() {
  local LIBDEFLATE0_URL="http://archive.ubuntu.com/ubuntu/pool/universe/libd/libdeflate/libdeflate0_1.5-3_amd64.deb"
  local LIBDEFLATE_DEV_URL="http://archive.ubuntu.com/ubuntu/pool/universe/libd/libdeflate/libdeflate-dev_1.5-3_amd64.deb"
  TMP_DIR=$(mktemp -d)
  wget -q -P "${TMP_DIR}" "${LIBDEFLATE0_URL}"
  dpkg -i "${TMP_DIR}/$(basename "${LIBDEFLATE0_URL}")"
  wget -q -P "${TMP_DIR}" "${LIBDEFLATE_DEV_URL}"
  dpkg -i "${TMP_DIR}/$(basename "${LIBDEFLATE_DEV_URL}")"
  rm -rf "${TMP_DIR}"
}

if [[ "${ROLE}" == 'Master' ]]; then
  if [[ ${OS_ID} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    remove_old_backports
  fi
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
  REPOSITORY_KEY=95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7
  run_with_retries apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys ${REPOSITORY_KEY}
  # https://cran.r-project.org/bin/linux/ubuntu/
  if [[ "${OS_ID}" == "ubuntu" ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc | tee -a /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc
  fi
  apt-get install -y software-properties-common
  add-apt-repository "deb http://cran.r-project.org/bin/linux/${OS_ID} ${OS_CODE}-cran40/"
  if [[ ${OS_ID} == ubuntu ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} < 2.1" | bc -l) == 1 ]]; then
    install_package
  fi
  update_apt_get
  apt-get install -y r-base r-base-dev gdebi-core

  # Download and install RStudio Server package:
  wget -nv --timeout=30 --tries=5 --retry-connrefused ${RSTUDIO_SERVER_PACKAGE_URI} -P /tmp
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
    service_file=/etc/systemd/system/rstudio-server.service
    if [[ "${OS_CODE}" == "bookworm" ]] || [[ "${OS_CODE}" == "jammy" ]];then
      service_file=/lib/systemd/system/rstudio-server.service
    fi
    sed -i 's:ExecStart=\(.*\):Environment=USER=rstudio\nExecStart=\1 --auth-none 1:1' "$service_file"
    systemctl daemon-reload
    systemctl restart rstudio-server
  fi
fi
