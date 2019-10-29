#!/bin/bash
# Copyright 2018 Google, Inc.
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

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly DEAFULT_INIT_ACTIONS_REPO="gs://dataproc-initialization-actions"
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO ||
  echo ${DEAFULT_INIT_ACTIONS_REPO})"
  readonly INIT_ACTIONS_BRANCH="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_BRANCH ||
  echo 'master')"

readonly JUPYTER_INIT_SCRIPT='/usr/lib/systemd/system/jupyter.service'
readonly CONDA_DIRECTORY='/opt/conda/anaconda'
readonly EXISTING_PYSPARK_KERNEL='pyspark'

function retry_apt_command() {
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
  retry_apt_command "apt-get update"
}

function install_apt_get() {
  pkgs="$@"
  retry_apt_command "apt-get install -y $pkgs"
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_sparkmonitor(){
  # Install asyncio
  retry_apt_command "/opt/conda/default/bin/pip install asyncio" ||
    err "Failed to install asyncio"

  # Remove existing PySpark kernel as it uses pyspark shell instead of ipython
  retry_apt_command "/opt/conda/default/bin/jupyter kernelspec remove ${EXISTING_PYSPARK_KERNEL} -f" ||
    err "Failed to remove existing PySpark kernel"

  # Install sparkmonitor. Don't mind if it fails to start the first time.
  retry_apt_command "/opt/conda/default/bin/pip install sparkmonitor"
  if [ $? != 0 ]; then
    err 'Failed to install sparkmonitor'
  fi
}

function configure_sparkmonitor(){
  local sparkmonitor_version;
  sparkmonitor_version="$(/opt/conda/default/bin/pip list | grep -i sparkmonitor | awk 'END {print $2}')"
  retry_apt_command "/opt/conda/default/bin/jupyter nbextension install sparkmonitor --py --system --symlink"
  retry_apt_command "/opt/conda/default/bin/jupyter nbextension enable sparkmonitor --py --system"
  retry_apt_command "/opt/conda/default/bin/jupyter serverextension enable --py --system sparkmonitor"
  retry_apt_command "/opt/conda/default/bin/ipython profile create"
  local ipython_profile_location="$(/opt/conda/default/bin/ipython profile locate default)"
  echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >>  ${ipython_profile_location}/ipython_kernel_config.py
}

function main() {
  if [[ ! -f "${JUPYTER_INIT_SCRIPT}" ]]; then
    err "Jupyter component is missing"
    exit 1
  fi

  if [[ ! -d "${CONDA_DIRECTORY}" ]]; then
    err "Anaconda component is missing"
    exit 1
  fi

  echo "Cloning initialization actions from '${INIT_ACTIONS_REPO}' repo..."
  INIT_ACTIONS_DIR=$(mktemp -d -t dataproc-init-actions-XXXX)
  readonly INIT_ACTIONS_DIR
  export INIT_ACTIONS_DIR
  if [[ ${INIT_ACTIONS_REPO} == gs://* ]]; then
    gsutil -m rsync -r "${INIT_ACTIONS_REPO}" "${INIT_ACTIONS_DIR}"
  else
    git clone -b "${INIT_ACTIONS_BRANCH}" --single-branch "${INIT_ACTIONS_REPO}" "${INIT_ACTIONS_DIR}"
  fi
  find "${INIT_ACTIONS_DIR}" -name '*.sh' -exec chmod +x {} \;

  # Ensure we have Conda installed.
  bash "${INIT_ACTIONS_DIR}/conda/bootstrap-conda.sh"

  if [[ -f /etc/profile.d/conda.sh ]]; then
    source /etc/profile.d/conda.sh
  fi

  if [[ -f /etc/profile.d/effective-python.sh ]]; then
    source /etc/profile.d/effective-python.sh
  fi

  if [[ "${ROLE}" == 'Master' ]]; then
    "${INIT_ACTIONS_DIR}/jupyter-sparkmonitor/internal/setup-jupyter-kernel.sh"
  fi

  update_apt_get || err 'Failed to update apt-get'
  install_sparkmonitor
  configure_sparkmonitor
  systemctl daemon-reload
  systemctl restart jupyter.service
}

main
