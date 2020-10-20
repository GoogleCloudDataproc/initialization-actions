#!/bin/bash

# Copyright 2019 Google LLC.
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

# This init script installs Apache Zeppelin on the master node of a Cloud
# Dataproc cluster. Zeppelin is also configured based on the size of your
# cluster and the versions of Spark/Hadoop which are installed.

set -euxo pipefail

readonly NOT_SUPPORTED_MESSAGE="Jupyter Spark Monitor initialization action is not supported on Dataproc ${DATAPROC_VERSION}.
Use Jupyter Component instead: https://cloud.google.com/dataproc/docs/concepts/components/jupyter"
[[ $DATAPROC_VERSION != 1.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

source '/usr/local/share/google/dataproc/bdutil/bdutil_helpers.sh'

readonly SPARKMONITOR_VERSION=0.0.11
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly JUPYTER_INIT_SCRIPT='/usr/lib/systemd/system/jupyter.service'
readonly CONDA_DIRECTORY='/opt/conda/default'
readonly PYTHON_PATH="${CONDA_DIRECTORY}/bin/python"
readonly EXISTING_PYSPARK_KERNEL='pyspark'
readonly DATAPROC_VERSION="$(grep DATAPROC_VERSION /etc/environment | cut -d= -f2 | sed -e 's/"//g')"

function retry_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_command "apt-get update"
}

function install_apt_get() {
  pkgs="$*"
  retry_command "apt-get install -y $pkgs"
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function install_sparkmonitor() {
  # Install asyncio
  retry_command "${CONDA_DIRECTORY}/bin/pip install asyncio" ||
    err "Failed to install asyncio"

  # Install sparkmonitor. Don't mind if it fails to start the first time.
  retry_command "/${CONDA_DIRECTORY}/bin/pip install 'sparkmonitor-s==${SPARKMONITOR_VERSION}'" ||
    err 'Failed to install sparkmonitor'
}

function configure_sparkmonitor() {
  ${CONDA_DIRECTORY}/bin/jupyter nbextension install sparkmonitor --py --system --symlink
  ${CONDA_DIRECTORY}/bin/jupyter nbextension enable sparkmonitor --py --system
  ${CONDA_DIRECTORY}/bin/jupyter serverextension enable --py --system sparkmonitor
  ${CONDA_DIRECTORY}/bin/ipython profile create
  local ipython_profile_location
  ipython_profile_location="$(${CONDA_DIRECTORY}/bin/ipython profile locate default)"
  echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >>"${ipython_profile_location}/ipython_kernel_config.py"
}

function main() {
  if if_version_at_least "${DATAPROC_VERSION}" "1.4"; then
    err "Must use Dataproc image version 1.4 or higher"
  fi

  if [[ "${ROLE}" != 'Master' ]]; then
    exit 0
  fi

  if [[ ! -f "${JUPYTER_INIT_SCRIPT}" ]]; then
    err "Jupyter component is missing"
  fi

  if [[ ! -d "${CONDA_DIRECTORY}" ]]; then
    err "Anaconda component is missing"
  fi

  if [[ -f /etc/profile.d/conda.sh ]]; then
    source /etc/profile.d/conda.sh
  fi

  if [[ -f /etc/profile.d/effective-python.sh ]]; then
    source /etc/profile.d/effective-python.sh
  fi

  update_apt_get || err 'Failed to update apt-get'
  install_sparkmonitor || err 'Failed to install Spark Monitor'
  configure_sparkmonitor || err 'Failed to configure Spark Monitor'

  systemctl daemon-reload
  systemctl restart jupyter.service
}

main
