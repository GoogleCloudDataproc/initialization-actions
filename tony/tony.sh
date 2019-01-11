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
#
# This script installs TonY on a master node within a Google Cloud Dataproc cluster.

set -x -e

readonly TONY_INSTALL_FOLDER='/usr/local/src'
readonly TONY_SAMPLES_FOLDER="${TONY_INSTALL_FOLDER}"'/TonY-samples'
readonly TENSORFLOW_VERSION='1.9'
readonly PYTORCH_VERSION='0.4.1'

# TonY configurations
readonly PS_INSTANCES=1
readonly PS_MEMORY='4g'
readonly WORKER_INSTANCES=2
readonly WORKER_MEMORY='4g'
readonly WORKER_GPUS=0     # Not supported in Dataproc


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function download_and_build_tony() {
  # Download TonY latest distribution.
  cd "${TONY_INSTALL_FOLDER}"
  git clone https://github.com/linkedin/TonY.git
  cd TonY
  # Build TonY without tests.
  ./gradlew build -x test
  return 0
}

function install_samples() {

  # Create samples directory structure.
  mkdir -p "${TONY_SAMPLES_FOLDER}"/deps
  # Create TensorFlow directory
  mkdir -p "${TONY_SAMPLES_FOLDER}"/jobs/TFJob/src
  # Create PyTorch directory
  mkdir -p "${TONY_SAMPLES_FOLDER}"/jobs/PTJob/src

  # Copy Jar file.
  cp "${TONY_INSTALL_FOLDER}"/TonY/tony-cli/build/libs/tony-cli-0.1.5-all.jar "${TONY_SAMPLES_FOLDER}"

  # Collect Metadata
  worker_instances="$(/usr/share/google/get_metadata_value attributes/worker_instances)" || worker_instances="${WORKER_INSTANCES}"
  worker_memory="$(/usr/share/google/get_metadata_value attributes/worker_memory)" || worker_memory="${WORKER_MEMORY}"
  ps_instances="$(/usr/share/google/get_metadata_value attributes/ps_instances)" || ps_instances="${PS_INSTANCES}"
  ps_memory="$(/usr/share/google/get_metadata_value attributes/ps_memory)" || ps_memory="${PS_MEMORY}"
  worker_gpus="$(/usr/share/google/get_metadata_value attributes/worker_gpus)" || worker_gpus="${WORKER_GPUS}"

  # Install TensorFlow sample
  cd "${TONY_SAMPLES_FOLDER}"/deps
  virtualenv -p python3 tf
  source tf/bin/activate
  pip install tensorflow=="${TENSORFLOW_VERSION}"
  zip -r tf.zip tf

  cp "${TONY_INSTALL_FOLDER}"/TonY/tony-examples/mnist-tensorflow/mnist_distributed.py \
    "${TONY_SAMPLES_FOLDER}"/jobs/TFJob/src
  cd "${TONY_SAMPLES_FOLDER}"/jobs/TFJob

  # Tony Configurations: https://github.com/linkedin/TonY/wiki/TonY-Configurations
  cat << EOF > tony.xml
<configuration>
 <property>
  <name>tony.application.security.enabled</name>
  <value>false</value>
 </property>
 <property>
  <name>tony.worker.instances</name>
  <value>${worker_instances}</value>
 </property>
 <property>
  <name>tony.worker.memory</name>
  <value>${worker_memory}</value>
 </property>
 <property>
  <name>tony.ps.instances</name>
  <value>${ps_instances}</value>
 </property>
 <property>
  <name>tony.ps.memory</name>
  <value>${ps_memory}</value>
 </property>
 <property>
  <name>tony.worker.gpus</name>
  <value>${worker_gpus}</value>
 </property>
</configuration>
EOF

  # Install PyTorch sample
  cd "${TONY_SAMPLES_FOLDER}"/deps
  virtualenv -p python3 pytorch
  source pytorch/bin/activate
  pip install torch=="${PYTORCH_VERSION}"
  zip -r pytorch.zip pytorch
  cp "${TONY_INSTALL_FOLDER}"/TonY/tony-examples/mnist-pytorch/mnist_distributed.py \
    "${TONY_SAMPLES_FOLDER}"/jobs/PTJob/src
  cd "${TONY_SAMPLES_FOLDER}"/jobs/PTJob/

  # Tony Configurations: https://github.com/linkedin/TonY/wiki/TonY-Configurations
  cat << EOF > tony.xml
<configuration>
 <property>
  <name>tony.application.name</name>
  <value>PyTorch</value>
 </property
 <property>
  <name>tony.application.security.enabled</name>
  <value>false</value>
 </property>
 <property>
  <name>tony.worker.instances</name>
  <value>${worker_instances}</value>
 </property>
 <property>
  <name>tony.worker.memory</name>
  <value>${worker_memory}</value>
 </property>
 <property>
  <name>tony.ps.instances</name>
  <value>${ps_instances}</value>
 </property>
 <property>
  <name>tony.ps.memory</name>
  <value>${ps_memory}</value>
 </property>
 <property>
  <name>tony.application.framework</name>
  <value>pytorch</value>
 </property>
 <property>
  <name>tony.worker.gpus</name>
  <value>${worker_gpus}</value>
 </property>
</configuration>
EOF

  echo 'TonY successfully added samples'
}

function main() {
  # Determine the role of this node
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  # Only run on the master node of the cluster
  if [[ "${role}" == 'Master' ]]; then
    download_and_build_tony || err "TonY install process failed"
    install_samples || err "Unable to install samples"
    echo 'TonY successfully deployed.'
  else
    echo 'TonY can be installed only on master node - skipped for worker node'
    return 0
  fi
}

main