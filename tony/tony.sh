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

set -euxo pipefail

# TonY settings
readonly TONY_INSTALL_FOLDER='/opt/tony'
readonly TONY_SAMPLES_FOLDER="${TONY_INSTALL_FOLDER}/TonY-samples"
readonly TONY_DEFAULT_VERSION='0.4.0'

# Tony configurations: https://github.com/linkedin/TonY/wiki/TonY-Configurations
readonly PS_INSTANCES=1
readonly PS_MEMORY='2g'
readonly WORKER_INSTANCES=2
readonly WORKER_MEMORY='4g'
readonly WORKER_GPUS=1

# ML frameworks versions
readonly TENSORFLOW_VERSION='2.4.1'
readonly TENSORFLOW_GPU=false

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly ROLE

function download_and_build_tony() {
  # Download TonY distribution.
  mkdir "${TONY_INSTALL_FOLDER}"
  cd "${TONY_INSTALL_FOLDER}"
  git clone https://github.com/linkedin/TonY.git
  cd TonY
  git checkout tags/v"${TONY_DEFAULT_VERSION}" -b "${TONY_DEFAULT_VERSION}"
  # Build TonY without tests.
  ./gradlew build -x test
}

function tf_gpu_config() {
  cat <<EOF >tony.xml
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
  <name>tony.worker.gpus</name>
  <value>${worker_gpus}</value>
 </property>
 <property>
  <name>tony.application.framework</name>
  <value>tensorflow</value>
 </property>
</configuration>
EOF
}


function tf_cpu_config() {
  cat <<EOF >tony.xml
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
  <name>tony.application.framework</name>
  <value>tensorflow</value>
 </property>
</configuration>
EOF
}

function install_samples() {
  # Create samples directory structure.
  mkdir -p "${TONY_SAMPLES_FOLDER}/deps"
  # Create TensorFlow directory
  mkdir -p "${TONY_SAMPLES_FOLDER}/jobs/TFJob/src"

  # Copy jar file.
  cp "${TONY_INSTALL_FOLDER}"/TonY/tony-cli/build/libs/tony-cli-${TONY_DEFAULT_VERSION}-uber.jar "${TONY_SAMPLES_FOLDER}"
  ln -s "${TONY_SAMPLES_FOLDER}"/tony-cli-${TONY_DEFAULT_VERSION}-uber.jar "${TONY_SAMPLES_FOLDER}/tony-cli.jar"

  # Collect metadata
  local worker_instances
  worker_instances="$(/usr/share/google/get_metadata_value attributes/worker_instances || echo ${WORKER_INSTANCES})"
  local worker_memory
  worker_memory="$(/usr/share/google/get_metadata_value attributes/worker_memory || echo ${WORKER_MEMORY})"
  local worker_gpus
  worker_gpus="$(/usr/share/google/get_metadata_value attributes/worker_gpus || echo ${WORKER_GPUS})"
  local ps_instances
  ps_instances="$(/usr/share/google/get_metadata_value attributes/ps_instances || echo ${PS_INSTANCES})"
  local ps_memory
  ps_memory="$(/usr/share/google/get_metadata_value attributes/ps_memory || echo ${PS_MEMORY})"

  # TensorFlow version
  local tf_version
  tf_version="$(/usr/share/google/get_metadata_value attributes/tf_version || echo ${TENSORFLOW_VERSION})"
  local tf_gpu
  tf_gpu="$(/usr/share/google/get_metadata_value attributes/tf_gpu || echo ${TENSORFLOW_GPU})"

  # Install TensorFlow sample
  cd "${TONY_SAMPLES_FOLDER}/deps"
  virtualenv -p python3 tf
  set +u
  source tf/bin/activate
  set -u

  # Verify you install GPU drivers, CUDA and CUDNN compatible with TensorFlow.
  if [[ "${tf_gpu}" == 'true' ]]; then
    if [[ "${tf_version}" == 'tf-nightly-gpu' ]]; then
      pip install "${tf_version}"
    else
      pip install "tensorflow-gpu==${tf_version}"
    fi
  else
    if [[ "${tf_version}" == 'tf-nightly' ]]; then
      pip install "${tf_version}"
    else
      pip install "tensorflow==${tf_version}"
    fi
  fi
  zip -r tf.zip tf

  cp "${TONY_INSTALL_FOLDER}/TonY/tony-examples/mnist-tensorflow/mnist_keras_distributed.py" \
    "${TONY_SAMPLES_FOLDER}/jobs/TFJob/src"
  cd "${TONY_SAMPLES_FOLDER}/jobs/TFJob"

  # Additional configuration settings: https://github.com/linkedin/TonY/wiki/TonY-Configurations
  if [[ "${tf_gpu}" == 'true' ]]; then
    tf_gpu_config 
  else
    tf_cpu_config
  fi

  echo 'TonY successfully added samples'
}

function main() {
  # Only run on the master node of the cluster
  if [[ "${ROLE}" == "Master" ]]; then
    download_and_build_tony
    install_samples
    echo 'TonY successfully deployed.'
  else
    echo 'TonY can be installed only on master node - skipped for worker node'
  fi
}

main
