#!/bin/bash
# Copyright 2019 Google, Inc.
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
  pkgs="$@"
  retry_command "apt-get install -y $pkgs"
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_sparkmonitor(){
  # Install asyncio
  retry_command "${CONDA_DIRECTORY}/bin/pip install asyncio" ||
    err "Failed to install asyncio"

  # Remove existing PySpark kernel as it uses pyspark shell instead of ipython
  ${CONDA_DIRECTORY}/bin/jupyter kernelspec remove ${EXISTING_PYSPARK_KERNEL} -f ||
    err "Failed to remove existing PySpark kernel"

  # Install sparkmonitor. Don't mind if it fails to start the first time.
  retry_command "/${CONDA_DIRECTORY}/bin/pip install sparkmonitor"
  if [ $? != 0 ]; then
    err 'Failed to install sparkmonitor'
  fi
}

function configure_sparkmonitor(){
  local sparkmonitor_version;
  sparkmonitor_version="$(${CONDA_DIRECTORY}/bin/pip list | grep -i sparkmonitor | awk 'END {print $2}')"
  ${CONDA_DIRECTORY}/bin/jupyter nbextension install sparkmonitor --py --system --symlink
  ${CONDA_DIRECTORY}/bin/jupyter nbextension enable sparkmonitor --py --system
  ${CONDA_DIRECTORY}/bin/jupyter serverextension enable --py --system sparkmonitor
  ${CONDA_DIRECTORY}/bin/ipython profile create
  local ipython_profile_location="$(${CONDA_DIRECTORY}/bin/ipython profile locate default)"
  echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >>  ${ipython_profile_location}/ipython_kernel_config.py
}

# Modify a PySpark kernel because Sparkmonitor requires IPython Kernel to be used.
# But the default PySpark kernel uses pyspark shell.
function install_pyspark_kernel() {
  echo "Installing pyspark Kernel..."
  local pyspark_kernel_tmp_dir=$(mktemp -d -t dataproc-init-actions-sparkmonitor-XXXX)

  cat << EOF >"${pyspark_kernel_tmp_dir}/kernel.json"
  {
   "argv": [
      "python", "-m", "ipykernel", "-f", "{connection_file}"],
   "display_name": "PySpark",
   "language": "python",
   "env": {
      "SPARK_HOME": "/usr/lib/spark/",
      "PYTHONPATH": "${PYTHON_PATH}"
   }
  }
EOF

  /opt/conda/default/bin/jupyter kernelspec install "${pyspark_kernel_tmp_dir}"
  rm -rf $pyspark_kernel_tmp_dir
  echo "PySpark kernel setup completed!"
}
function main() {
  if [[ ${DATAPROC_VERSION} < '1.4' ]]; then
    err "Must use Dataproc image version 1.4 or higher"
    exit 1
  fi

  if [[ "${ROLE}" != 'Master' ]]; then
    exit 0
  fi

  if [[ ! -f "${JUPYTER_INIT_SCRIPT}" ]]; then
    err "Jupyter component is missing"
    exit 1
  fi

  if [[ ! -d "${CONDA_DIRECTORY}" ]]; then
    err "Anaconda component is missing"
    exit 1
  fi

  if [[ -f /etc/profile.d/conda.sh ]]; then
    source /etc/profile.d/conda.sh
  fi

  if [[ -f /etc/profile.d/effective-python.sh ]]; then
    source /etc/profile.d/effective-python.sh
  fi

  update_apt_get || err 'Failed to update apt-get'
  install_sparkmonitor
  configure_sparkmonitor
  install_pyspark_kernel

  systemctl daemon-reload
  systemctl restart jupyter.service
}

main
