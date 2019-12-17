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
readonly TONY_DEFAULT_VERSION='0c4bd4095d21b2d1ebbf7441d59fe42022714464'

# Tony configurations: https://github.com/linkedin/TonY/wiki/TonY-Configurations
readonly PS_INSTANCES=1
readonly PS_MEMORY='2g'
readonly WORKER_INSTANCES=2
readonly WORKER_MEMORY='4g'
readonly WORKER_GPUS=0 # GPU isolation is not supported in Dataproc 1.3

# ML frameworks versions
readonly TENSORFLOW_VERSION='1.13.2'
readonly TENSORFLOW_GPU=false
readonly TORCH_VERSION='0.4.1.post2'
readonly TORCHVISION_VERSION='0.2.1'

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly ROLE

function download_and_build_tony() {
  # Download TonY distribution.
  mkdir "${TONY_INSTALL_FOLDER}"
  cd "${TONY_INSTALL_FOLDER}"
  git clone https://github.com/linkedin/TonY.git
  cd TonY
  git checkout "${TONY_DEFAULT_VERSION}"
  # Build TonY without tests.
  ./gradlew build -x test
}

function install_samples() {
  # Create samples directory structure.
  mkdir -p "${TONY_SAMPLES_FOLDER}/deps"
  # Create TensorFlow directory
  mkdir -p "${TONY_SAMPLES_FOLDER}/jobs/TFJob/src"
  # Create PyTorch directory
  mkdir -p "${TONY_SAMPLES_FOLDER}/jobs/PTJob/src"

  # Copy jar file.
  cp "${TONY_INSTALL_FOLDER}"/TonY/tony-cli/build/libs/tony-cli-*-all.jar "${TONY_SAMPLES_FOLDER}"
  ln -s "${TONY_SAMPLES_FOLDER}"/tony-cli-*-all.jar "${TONY_SAMPLES_FOLDER}/tony-cli-all.jar"

  # Collect metadata
  local worker_instances
  worker_instances="$(/usr/share/google/get_metadata_value attributes/worker_instances || echo ${WORKER_INSTANCES})"
  local worker_memory
  worker_memory="$(/usr/share/google/get_metadata_value attributes/worker_memory || echo ${WORKER_MEMORY})"
  local ps_instances
  ps_instances="$(/usr/share/google/get_metadata_value attributes/ps_instances || echo ${PS_INSTANCES})"
  local ps_memory
  ps_memory="$(/usr/share/google/get_metadata_value attributes/ps_memory || echo ${PS_MEMORY})"

  # TensorFlow version
  local tf_version
  tf_version="$(/usr/share/google/get_metadata_value attributes/tf_version || echo ${TENSORFLOW_VERSION})"
  local tf_gpu
  tf_gpu="$(/usr/share/google/get_metadata_value attributes/tf_gpu || echo ${TENSORFLOW_GPU})"

  # PyTorch version
  local torch_version
  torch_version="$(/usr/share/google/get_metadata_value attributes/torch_version || echo ${TORCH_VERSION})"
  local torchvision_version
  torchvision_version="$(/usr/share/google/get_metadata_value attributes/torchvision_version || echo ${TORCHVISION_VERSION})"

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

  cp "${TONY_INSTALL_FOLDER}/TonY/tony-examples/mnist-tensorflow/mnist_distributed.py" \
    "${TONY_SAMPLES_FOLDER}/jobs/TFJob/src"
  cd "${TONY_SAMPLES_FOLDER}/jobs/TFJob"

  # Additional configuration settings: https://github.com/linkedin/TonY/wiki/TonY-Configurations
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
  <name>tony.ps.instances</name>
  <value>${ps_instances}</value>
 </property>
 <property>
  <name>tony.ps.memory</name>
  <value>${ps_memory}</value>
 </property>
 <property>
  <name>tony.worker.gpus</name>
  <value>${WORKER_GPUS}</value>
 </property>
</configuration>
EOF

  # Install PyTorch sample
  cd "${TONY_SAMPLES_FOLDER}/deps"
  virtualenv -p python3 pytorch
  set +u
  source pytorch/bin/activate
  set -u

  pip install "torch==${torch_version}" "torchvision==${torchvision_version}"
  pip install -I numpy
  zip -r pytorch.zip pytorch
  cp "${TONY_INSTALL_FOLDER}/TonY/tony-examples/mnist-pytorch/mnist_distributed.py" \
    "${TONY_SAMPLES_FOLDER}/jobs/PTJob/src"
  cd "${TONY_SAMPLES_FOLDER}/jobs/PTJob/"

  # Additional configuration settings: https://github.com/linkedin/TonY/wiki/TonY-Configurations
  cat <<EOF >tony.xml
<configuration>
 <property>
  <name>tony.application.name</name>
  <value>PyTorch</value>
 </property>
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
  <value>${WORKER_GPUS}</value>
 </property>
</configuration>
EOF

  echo 'TonY successfully added samples'
}

# Only run on the master node of the cluster
if [[ "${ROLE}" == "Master" ]]; then
  download_and_build_tony
  install_samples
  echo 'TonY successfully deployed.'
else
  echo 'TonY can be installed only on master node - skipped for worker node'
fi
