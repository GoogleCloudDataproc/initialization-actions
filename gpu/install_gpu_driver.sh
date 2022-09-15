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
# This script installs NVIDIA GPU drivers and collects GPU utilization metrics.

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_NAME

# CUDA version and Driver version
RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
if [[ ${DATAPROC_IMAGE_VERSION} == 2.* ]] && [[ "${RUNTIME}" == "SPARK" ]]; then
  CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '11.5')
  readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION='495.29.05'
  readonly DEFAULT_NCCL_VERSION="2.11.4"
  readonly DEFAULT_NCCL_VERSION_ROCKY="2.11.4"
else
  CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '11.2')
  readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION='460.73.01'
  readonly DEFAULT_NCCL_VERSION="2.8.3"
  readonly DEFAULT_NCCL_VERSION_ROCKY="2.8.4"
fi
readonly CUDA_VERSION
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION_PREFIX=${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION%%.*}

# Dataproc node role
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly ROLE

# Parameters for NVIDIA-provided Debian GPU driver
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION}.run"
NVIDIA_DEBIAN_GPU_DRIVER_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL}")
readonly NVIDIA_DEBIAN_GPU_DRIVER_URL

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'

# Parameters for NVIDIA-provided NCCL library
readonly DEFAULT_NCCL_REPO_URL="${NVIDIA_BASE_DL_URL}/machine-learning/repos/ubuntu1804/x86_64/nvidia-machine-learning-repo-ubuntu1804_1.0.0-1_amd64.deb"
NCCL_REPO_URL=$(get_metadata_attribute 'nccl-repo-url' "${DEFAULT_NCCL_REPO_URL}")
readonly NCCL_REPO_URL
readonly NCCL_REPO_KEY="${NVIDIA_BASE_DL_URL}/machine-learning/repos/ubuntu1804/x86_64/7fa2af80.pub"

if [[ ${OS_NAME} == rocky ]]; then
  NCCL_VERSION=$(get_metadata_attribute 'nccl-version' ${DEFAULT_NCCL_VERSION_ROCKY})
else
  NCCL_VERSION=$(get_metadata_attribute 'nccl-version' ${DEFAULT_NCCL_VERSION})
fi
readonly NCCL_VERSION

readonly -A DEFAULT_NVIDIA_DEBIAN_CUDA_URLS=(
  [10.1]="${NVIDIA_BASE_DL_URL}/cuda/10.1/Prod/local_installers/cuda_10.1.243_418.87.00_linux.run"
  [10.2]="${NVIDIA_BASE_DL_URL}/cuda/10.2/Prod/local_installers/cuda_10.2.89_440.33.01_linux.run"
  [11.0]="${NVIDIA_BASE_DL_URL}/cuda/11.0.3/local_installers/cuda_11.0.3_450.51.06_linux.run"
  [11.1]="${NVIDIA_BASE_DL_URL}/cuda/11.1.0/local_installers/cuda_11.1.0_455.23.05_linux.run"
  [11.2]="${NVIDIA_BASE_DL_URL}/cuda/11.2.2/local_installers/cuda_11.2.2_460.32.03_linux.run"
  [11.5]="${NVIDIA_BASE_DL_URL}/cuda/11.5.2/local_installers/cuda_11.5.2_495.29.05_linux.run"
  [11.6]="${NVIDIA_BASE_DL_URL}/cuda/11.6.2/local_installers/cuda_11.6.2_510.47.03_linux.run"
  [11.7]="${NVIDIA_BASE_DL_URL}/cuda/11.7.1/local_installers/cuda_11.7.1_515.65.01_linux.run")
readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL=${DEFAULT_NVIDIA_DEBIAN_CUDA_URLS["${CUDA_VERSION}"]}
NVIDIA_DEBIAN_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_DEBIAN_CUDA_URL}")
readonly NVIDIA_DEBIAN_CUDA_URL

# Parameters for NVIDIA-provided Ubuntu GPU driver
readonly NVIDIA_UBUNTU_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu1804/x86_64"
readonly NVIDIA_UBUNTU_REPO_KEY_PACKAGE="${NVIDIA_UBUNTU_REPO_URL}/cuda-keyring_1.0-1_all.deb"
readonly NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu1804.pin"

# Parameter for NVIDIA-provided Rocky Linux GPU driver
readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/rhel8/x86_64/cuda-rhel8.repo"

# Parameters for NVIDIA-provided CUDNN library
readonly CUDNN_VERSION=$(get_metadata_attribute 'cudnn-version' '')
readonly CUDNN_TARBALL="cudnn-${CUDA_VERSION}-linux-x64-v${CUDNN_VERSION}.tgz"
readonly CUDNN_TARBALL_URL="http://developer.download.nvidia.com/compute/redist/cudnn/v${CUDNN_VERSION%.*}/${CUDNN_TARBALL}"

# Whether to install NVIDIA-provided or OS-provided GPU driver
GPU_DRIVER_PROVIDER=$(get_metadata_attribute 'gpu-driver-provider' 'NVIDIA')
readonly GPU_DRIVER_PROVIDER

# Stackdriver GPU agent parameters
readonly GPU_AGENT_REPO_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics'
# Whether to install GPU monitoring agent that sends GPU metrics to Stackdriver
INSTALL_GPU_AGENT=$(get_metadata_attribute 'install-gpu-agent' 'false')
readonly INSTALL_GPU_AGENT

# Dataproc configurations
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly SPARK_CONF_DIR='/etc/spark/conf'

NVIDIA_SMI_PATH='/usr/bin'
MIG_MAJOR_CAPS=0
IS_MIG_ENABLED=0

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_nvidia_nccl() {
  local -r nccl_version="${NCCL_VERSION}-1+cuda${CUDA_VERSION}"

  if [[ ${OS_NAME} == rocky ]]; then
    execute_with_retries "dnf -y -q install libnccl-${nccl_version} libnccl-devel-${nccl_version} libnccl-static-${nccl_version}"
  elif [[ ${OS_NAME} == ubuntu ]] || [[ ${OS_NAME} == debian ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 "${NCCL_REPO_KEY}" | apt-key add -

    local tmp_dir
    tmp_dir=$(mktemp -d -t gpu-init-action-nccl-XXXX)

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NCCL_REPO_URL}" -o "${tmp_dir}/nvidia-ml-repo.deb"
    dpkg -i "${tmp_dir}/nvidia-ml-repo.deb"

    execute_with_retries "apt-get update"

    execute_with_retries \
      "apt-get install -y --allow-unauthenticated libnccl2=${nccl_version} libnccl-dev=${nccl_version}"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
}

function install_nvidia_cudnn() {
  local major_version
  major_version="${CUDNN_VERSION%%.*}"
  local cudnn_pkg_version
  cudnn_pkg_version="${CUDNN_VERSION}-1+cuda${CUDA_VERSION}"

  if [[ ${OS_NAME} == rocky ]]; then
    if [[ ${major_version} == 8 ]]; then
      execute_with_retries "dnf -y -q install libcudnn8-${cudnn_pkg_version} libcudnn8-devel-${cudnn_pkg_version}"
    else
      echo "Unsupported CUDNN version: '${CUDNN_VERSION}'"
      exit 1
    fi
  elif [[ ${OS_NAME} == ubuntu ]]; then
    local -a packages
    packages=(
      "libcudnn${major_version}=${cudnn_pkg_version}"
      "libcudnn${major_version}-dev=${cudnn_pkg_version}")
    execute_with_retries \
      "apt-get install -y --no-install-recommends ${packages[*]}"
  else
    local tmp_dir
    tmp_dir=$(mktemp -d -t gpu-init-action-cudnn-XXXX)

    curl -fSsL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${CUDNN_TARBALL_URL}" -o "${tmp_dir}/${CUDNN_TARBALL}"

    tar -xzf "${tmp_dir}/${CUDNN_TARBALL}" -C /usr/local

    cat <<'EOF' >>/etc/profile.d/cudnn.sh
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:${LD_LIBRARY_PATH}
EOF
  fi

  ldconfig

  echo "NVIDIA cuDNN successfully installed for ${OS_NAME}."
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  if [[ ${OS_NAME} == debian ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
    dpkg -i "/tmp/cuda-keyring.deb"

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_DEBIAN_GPU_DRIVER_URL}" -o driver.run
    bash "./driver.run" --silent --install-libglvnd

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_DEBIAN_CUDA_URL}" -o cuda.run
    bash "./cuda.run" --silent --toolkit --no-opengl-libs
  elif [[ ${OS_NAME} == ubuntu ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
    dpkg -i "/tmp/cuda-keyring.deb"
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_UBUNTU_REPO_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

    add-apt-repository "deb ${NVIDIA_UBUNTU_REPO_URL} /"
    execute_with_retries "apt-get update"

    if [[ -n "${CUDA_VERSION}" ]]; then
      local -r cuda_package=cuda-toolkit-${CUDA_VERSION//./-}
    else
      local -r cuda_package=cuda-toolkit
    fi
    # Without --no-install-recommends this takes a very long time.
    execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION_PREFIX}"
    execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_package}"
  elif [[ ${OS_NAME} == rocky ]]; then
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"
    execute_with_retries "dnf -y -q module install nvidia-driver:${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION_PREFIX}-dkms"
    execute_with_retries "dnf -y -q install cuda-${CUDA_VERSION//./-}"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
  ldconfig
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  if ! command -v pip; then
    execute_with_retries "apt-get install -y -q python-pip"
  fi
  local install_dir=/opt/gpu-utilization-agent
  mkdir "${install_dir}"
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${GPU_AGENT_REPO_URL}/requirements.txt" -o "${install_dir}/requirements.txt"
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${GPU_AGENT_REPO_URL}/report_gpu_metrics.py" -o "${install_dir}/report_gpu_metrics.py"
  pip install -r "${install_dir}/requirements.txt"

  # Generate GPU service.
  cat <<EOF >/lib/systemd/system/gpu-utilization-agent.service
[Unit]
Description=GPU Utilization Metric Agent

[Service]
Type=simple
PIDFile=/run/gpu_agent.pid
ExecStart=/bin/bash --login -c 'python "${install_dir}/report_gpu_metrics.py"'
User=root
Group=root
WorkingDirectory=/
Restart=always

[Install]
WantedBy=multi-user.target
EOF
  # Reload systemd manager configuration
  systemctl daemon-reload
  # Enable gpu-utilization-agent service
  systemctl --no-reload --now enable gpu-utilization-agent.service
}

function set_hadoop_property() {
  local -r config_file=$1
  local -r property=$2
  local -r value=$3
  bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/${config_file}" \
    --name "${property}" --value "${value}" \
    --clobber
}

function configure_yarn() {
  if [[ ! -f ${HADOOP_CONF_DIR}/resource-types.xml ]]; then
    printf '<?xml version="1.0" ?>\n<configuration/>' >"${HADOOP_CONF_DIR}/resource-types.xml"
  fi
  set_hadoop_property 'resource-types.xml' 'yarn.resource-types' 'yarn.io/gpu'

  set_hadoop_property 'capacity-scheduler.xml' \
    'yarn.scheduler.capacity.resource-calculator' \
    'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator'

  set_hadoop_property 'yarn-site.xml' 'yarn.resource-types' 'yarn.io/gpu'
}

# This configuration should be applied only if GPU is attached to the node
function configure_yarn_nodemanager() {
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.resource-plugins' 'yarn.io/gpu'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices' 'auto'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables' $NVIDIA_SMI_PATH
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.mount' 'true'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.mount-path' '/sys/fs/cgroup'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.linux-container-executor.cgroups.hierarchy' 'yarn'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.container-executor.class' \
    'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor'
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.linux-container-executor.group' 'yarn'

  # Fix local dirs access permissions
  local yarn_local_dirs=()
  readarray -d ',' yarn_local_dirs < <(bdconfig get_property_value \
    --configuration_file "${HADOOP_CONF_DIR}/yarn-site.xml" \
    --name "yarn.nodemanager.local-dirs" 2>/dev/null | tr -d '\n')
  chown yarn:yarn -R "${yarn_local_dirs[@]/,/}"
}

function configure_gpu_exclusive_mode() {
  # check if running spark 3, if not, enable GPU exclusive mode
  local spark_version
  spark_version=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
  if [[ ${spark_version} != 3.* ]]; then
    # include exclusive mode on GPU
    nvidia-smi -c EXCLUSIVE_PROCESS
  fi
}

function fetch_mig_scripts() {
  mkdir -p /usr/local/yarn-mig-scripts
  sudo chmod 755 /usr/local/yarn-mig-scripts
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.06/examples/MIG-Support/yarn-unpatched/scripts/nvidia-smi
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.06/examples/MIG-Support/yarn-unpatched/scripts/mig2gpu.sh
  sudo chmod 755 /usr/local/yarn-mig-scripts/*
}

function configure_gpu_script() {
  # Download GPU discovery script
  local -r spark_gpu_script_dir='/usr/lib/spark/scripts/gpu'
  mkdir -p ${spark_gpu_script_dir}
  # need to update the getGpusResources.sh script to look for MIG devices since if multiple GPUs nvidia-smi still
  # lists those because we only disable the specific GIs via CGROUPs. Here we just create it based off of:
  # https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh
  echo '
#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
NUM_MIG_DEVICES=$(nvidia-smi -L | grep MIG | wc -l)
ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader | sed -e '\'':a'\'' -e '\''N'\'' -e'\''$!ba'\'' -e '\''s/\n/","/g'\'')
if [ $NUM_MIG_DEVICES -gt 0 ]; then
  MIG_INDEX=$(( $NUM_MIG_DEVICES - 1 ))
  ADDRS=$(seq -s '\''","'\'' 0 $MIG_INDEX)
fi
echo {\"name\": \"gpu\", \"addresses\":[\"$ADDRS\"]}
' > ${spark_gpu_script_dir}/getGpusResources.sh

  chmod a+rwx -R ${spark_gpu_script_dir}
}

function configure_gpu_isolation() {
  # enable GPU isolation
  sed -i "s/yarn\.nodemanager\.linux\-container\-executor\.group\=.*$/yarn\.nodemanager\.linux\-container\-executor\.group\=yarn/g" "${HADOOP_CONF_DIR}/container-executor.cfg"
  if [[ $IS_MIG_ENABLED -ne 0 ]]; then
    # configure the container-executor.cfg to have major caps
    printf '\n[gpu]\nmodule.enabled=true\ngpu.major-device-number=%s\n\n[cgroups]\nroot=/sys/fs/cgroup\nyarn-hierarchy=yarn\n' $MIG_MAJOR_CAPS >> "${HADOOP_CONF_DIR}/container-executor.cfg"
    printf 'export MIG_AS_GPU_ENABLED=1\n' >> "${HADOOP_CONF_DIR}/yarn-env.sh"
    printf 'export ENABLE_MIG_GPUS_FOR_CGROUPS=1\n' >> "${HADOOP_CONF_DIR}/yarn-env.sh"
  else
    printf '\n[gpu]\nmodule.enabled=true\n[cgroups]\nroot=/sys/fs/cgroup\nyarn-hierarchy=yarn\n' >> "${HADOOP_CONF_DIR}/container-executor.cfg"
  fi

  # Configure a systemd unit to ensure that permissions are set on restart
  cat >/etc/systemd/system/dataproc-cgroup-device-permissions.service<<EOF
[Unit]
Description=Set permissions to allow YARN to access device directories

[Service]
ExecStart=/bin/bash -c "chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct; chmod a+rwx -R /sys/fs/cgroup/devices"

[Install]
WantedBy=multi-user.target
EOF

  systemctl enable dataproc-cgroup-device-permissions
  systemctl start dataproc-cgroup-device-permissions
}

function main() {
  if [[ ${OS_NAME} != debian ]] && [[ ${OS_NAME} != ubuntu ]] && [[ ${OS_NAME} != rocky ]]; then
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
    export DEBIAN_FRONTEND=noninteractive
    execute_with_retries "apt-get update"
    execute_with_retries "apt-get install -y -q pciutils"
  elif [[ ${OS_NAME} == rocky ]] ; then
    execute_with_retries "dnf -y -q update"
    execute_with_retries "dnf -y -q install pciutils"
    execute_with_retries "dnf -y -q install kernel-devel"
    execute_with_retries "dnf -y -q install gcc"
  fi

  # This configuration should be ran on all nodes
  # regardless if they have attached GPUs
  configure_yarn

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    # if this is called without the MIG script then the drivers are not installed
    if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l); then
      NUM_MIG_GPUS=`/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l`
      if [[ $NUM_MIG_GPUS -eq 1 ]]; then
        if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | grep Enabled); then
          IS_MIG_ENABLED=1
          NVIDIA_SMI_PATH='/usr/local/yarn-mig-scripts/'
          MIG_MAJOR_CAPS=`grep nvidia-caps /proc/devices | cut -d ' ' -f 1`
          fetch_mig_scripts
        fi
      fi
    fi

    if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
      execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    fi

    # if mig is enabled drivers would have already been installed
    if [[ $IS_MIG_ENABLED -eq 0 ]]; then
      install_nvidia_gpu_driver
      if [[ -n ${CUDNN_VERSION} ]]; then
        install_nvidia_nccl
        install_nvidia_cudnn
      fi
      #Install GPU metrics collection in Stackdriver if needed
      if [[ ${INSTALL_GPU_AGENT} == true ]]; then
        install_gpu_agent
        echo 'GPU metrics agent successfully deployed.'
      else
        echo 'GPU metrics agent will not be installed.'
      fi
      configure_gpu_exclusive_mode
    fi

    configure_yarn_nodemanager
    configure_gpu_script
    configure_gpu_isolation
  elif [[ "${ROLE}" == "Master" ]]; then
    configure_yarn_nodemanager
    configure_gpu_script
  fi

  # Restart YARN services if they are running already
  if [[ $(systemctl show hadoop-yarn-resourcemanager.service -p SubState --value) == 'running' ]]; then
    systemctl restart hadoop-yarn-resourcemanager.service
  fi
  if [[ $(systemctl show hadoop-yarn-nodemanager.service -p SubState --value) == 'running' ]]; then
    systemctl restart hadoop-yarn-nodemanager.service
  fi
}

main
