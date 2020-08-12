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
OS_DIST=$(lsb_release -cs)
readonly OS_DIST

# Dataproc role
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly ROLE

# CUDA Version
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '10.2')
readonly CUDA_VERSION

# Parameters for NVIDIA-provided Debian GPU driver
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL='http://us.download.nvidia.com/XFree86/Linux-x86_64/450.51/NVIDIA-Linux-x86_64-450.51.run'
NVIDIA_DEBIAN_GPU_DRIVER_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL}")
readonly NVIDIA_DEBIAN_GPU_DRIVER_URL
readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL_10_1='http://developer.download.nvidia.com/compute/cuda/10.1/Prod/local_installers/cuda_10.1.243_418.87.00_linux.run'
readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL_10_2='http://developer.download.nvidia.com/compute/cuda/10.2/Prod/local_installers/cuda_10.2.89_440.33.01_linux.run'

if [[ "${CUDA_VERSION}" == "10.1" ]]; then
  readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL=${DEFAULT_NVIDIA_DEBIAN_CUDA_URL_10_1}
else
  readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL=${DEFAULT_NVIDIA_DEBIAN_CUDA_URL_10_2}
fi

NVIDIA_DEBIAN_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_DEBIAN_CUDA_URL}")
readonly NVIDIA_DEBIAN_CUDA_URL

# Parameters for NVIDIA-provided Ubuntu GPU driver
readonly NVIDIA_UBUNTU_REPOSITORY_URL='https://developer.download.nvidia.com/compute/cuda/repos/ubuntu1804/x86_64'
readonly NVIDIA_UBUNTU_REPOSITORY_KEY="${NVIDIA_UBUNTU_REPOSITORY_URL}/7fa2af80.pub"
readonly NVIDIA_UBUNTU_REPOSITORY_CUDA_PIN="${NVIDIA_UBUNTU_REPOSITORY_URL}/cuda-ubuntu1804.pin"

# Parameters for NVIDIA-provided NCCL library
readonly DEFAULT_NCCL_REPO_URL='https://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu1804/x86_64/nvidia-machine-learning-repo-ubuntu1804_1.0.0-1_amd64.deb'
NCCL_REPO_URL=$(get_metadata_attribute 'nccl-repo-url' "${DEFAULT_NCCL_REPO_URL}")
readonly NCCL_REPO_URL
NCCL_VERSION=$(get_metadata_attribute 'nccl-version' '2.7.6')
readonly NCCL_VERSION

# Parameters for Ubuntu-provided NVIDIA GPU driver
readonly NVIDIA_DRIVER_VERSION_UBUNTU='440'

# Whether to install NVIDIA-provided or OS-provided GPU driver
GPU_DRIVER_PROVIDER=$(get_metadata_attribute 'gpu-driver-provider' 'OS')
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
  local tmp_dir
  tmp_dir=$(mktemp -d -t gpu-init-action-nccl-XXXX)

  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NCCL_REPO_URL}" -o "${tmp_dir}/nvidia-ml-repo.deb"
  dpkg -i "${tmp_dir}/nvidia-ml-repo.deb"

  execute_with_retries "apt-get update"

  local -r nccl_version="${NCCL_VERSION}-1+cuda${CUDA_VERSION}"
  execute_with_retries \
    "apt-get install -y --allow-unauthenticated libnccl2=${nccl_version} libnccl-dev=${nccl_version}"
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_UBUNTU_REPOSITORY_KEY}" | apt-key add -
  if [[ ${OS_NAME} == debian ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_DEBIAN_GPU_DRIVER_URL}" -o driver.run
    bash "./driver.run" --silent --install-libglvnd

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_DEBIAN_CUDA_URL}" -o cuda.run
    bash "./cuda.run" --silent --toolkit --no-opengl-libs
  elif [[ ${OS_NAME} == ubuntu ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_UBUNTU_REPOSITORY_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

    add-apt-repository "deb ${NVIDIA_UBUNTU_REPOSITORY_URL} /"
    execute_with_retries "apt-get update"

    if [[ -n "${CUDA_VERSION}" ]]; then
      local -r cuda_package=cuda-${CUDA_VERSION//./-}
    else
      local -r cuda_package=cuda
    fi
    # Without --no-install-recommends this takes a very long time.
    execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_package}"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
}

# Install NVIDIA GPU driver provided by OS distribution
function install_os_gpu_driver() {
  local packages=(nvidia-cuda-toolkit)
  local modules=(nvidia-drm nvidia-uvm drm)

  # Add non-free Debian packages.
  # See https://www.debian.org/distrib/packages#note
  if [[ ${OS_NAME} == debian ]]; then
    for type in deb deb-src; do
      for distro in ${OS_DIST} ${OS_DIST}-backports; do
        echo "${type} http://deb.debian.org/debian ${distro} contrib non-free" \
          >>/etc/apt/sources.list.d/non-free.list
      done
    done

    packages+=(nvidia-driver nvidia-kernel-common nvidia-smi)
    modules+=(nvidia-current)
    local -r nvblas_cpu_blas_lib=/usr/lib/libblas.so
  elif [[ ${OS_NAME} == ubuntu ]]; then
    # Ubuntu-specific NVIDIA driver packages and modules
    packages+=("nvidia-driver-${NVIDIA_DRIVER_VERSION_UBUNTU}"
      "nvidia-kernel-common-${NVIDIA_DRIVER_VERSION_UBUNTU}")
    modules+=(nvidia)
    local -r nvblas_cpu_blas_lib=/usr/lib/x86_64-linux-gnu/libblas.so
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  # Install proprietary NVIDIA drivers and CUDA
  # See https://wiki.debian.org/NvidiaGraphicsDrivers
  # Without --no-install-recommends this takes a very long time.
  execute_with_retries "apt-get update"
  execute_with_retries \
    "apt-get install -y -q -t ${OS_DIST}-backports --no-install-recommends ${packages[*]}"

  # Create a system wide NVBLAS config
  # See http://docs.nvidia.com/cuda/nvblas/
  local -r nvblas_config_file=/etc/nvidia/nvblas.conf
  # Create config file if it does not exist - this file doesn't exist by default in Ubuntu
  mkdir -p "$(dirname ${nvblas_config_file})"
  cat <<EOF >>${nvblas_config_file}
# Insert here the CPU BLAS fallback library of your choice.
# The standard libblas.so.3 defaults to OpenBLAS, which does not have the
# requisite CBLAS API.
NVBLAS_CPU_BLAS_LIB ${nvblas_cpu_blas_lib}
# Use all GPUs
NVBLAS_GPU_LIST ALL
# Add more configuration here.
EOF
  echo "NVBLAS_CONFIG_FILE=${nvblas_config_file}" >>/etc/environment

  # Rebooting during an initialization action is not recommended, so just
  # dynamically load kernel modules. If you want to run an X server, it is
  # recommended that you schedule a reboot to occur after the initialization
  # action finishes.
  modprobe -r nouveau
  modprobe "${modules[@]}"

  # Restart any NodeManagers, so they pick up the NVBLAS config.
  if systemctl status hadoop-yarn-nodemanager; then
    # Kill Node Manager to prevent unregister/register cycle
    systemctl kill -s KILL hadoop-yarn-nodemanager
  fi

  echo "NVIDIA GPU driver provided by ${OS_NAME} was installed successfully"
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
  systemctl --now enable gpu-utilization-agent.service
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
  set_hadoop_property 'capacity-scheduler.xml' \
    'yarn.scheduler.capacity.resource-calculator' \
    'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator'

  set_hadoop_property 'yarn-site.xml' 'yarn.resource-types' 'yarn.io/gpu'
  set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.resource-plugins' 'yarn.io/gpu'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.allowed-gpu-devices' 'auto'
  set_hadoop_property 'yarn-site.xml' \
    'yarn.nodemanager.resource-plugins.gpu.path-to-discovery-executables' '/usr/bin'
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

  local yarn_local_dirs=()
  readarray -d ',' yarn_local_dirs < <(bdconfig get_property_value \
    --configuration_file "/etc/hadoop/conf/yarn-site.xml" \
    --name "yarn.nodemanager.local-dirs" 2>/dev/null | tr -d '\n')
  chown yarn:yarn -R "${yarn_local_dirs[@]/,}"
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

function configure_gpu_isolation() {
  # download GPU discovery script
  local -r spark_gpu_script_dir='/usr/lib/spark/scripts/gpu'
  mkdir -p ${spark_gpu_script_dir}
  local -r gpu_resources_url=https://raw.githubusercontent.com/apache/spark/master/examples/src/main/scripts/getGpusResources.sh
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${gpu_resources_url}" -o ${spark_gpu_script_dir}/getGpusResources.sh
  chmod a+rwx -R ${spark_gpu_script_dir}

  # enable GPU isolation
  sed -i "s/yarn.nodemanager\.linux\-container\-executor\.group\=/yarn\.nodemanager\.linux\-container\-executor\.group\=yarn/g" /etc/hadoop/conf/container-executor.cfg
  printf '\n[gpu]\nmodule.enabled=true\n[cgroups]\nroot=/sys/fs/cgroup\nyarn-hierarchy=yarn\n' >>/etc/hadoop/conf/container-executor.cfg

  chmod a+rwx -R /sys/fs/cgroup/cpu,cpuacct
  chmod a+rwx -R /sys/fs/cgroup/devices
}

function main() {
  if [[ ${OS_NAME} != debian ]] && [[ ${OS_NAME} != ubuntu ]]; then
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  export DEBIAN_FRONTEND=noninteractive
  execute_with_retries "apt-get update"
  execute_with_retries "apt-get install -y -q pciutils"

  configure_gpu_isolation
  configure_yarn

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    if [[ ${GPU_DRIVER_PROVIDER} == 'NVIDIA' ]]; then
      install_nvidia_gpu_driver
      install_nvidia_nccl
    elif [[ ${GPU_DRIVER_PROVIDER} == 'OS' ]]; then
      install_os_gpu_driver
    else
      echo "Unsupported GPU driver provider: '${GPU_DRIVER_PROVIDER}'"
      exit 1
    fi

    # Install GPU metrics collection in Stackdriver if needed
    if [[ ${INSTALL_GPU_AGENT} == true ]]; then
      install_gpu_agent
      echo 'GPU agent successfully deployed.'
    else
      echo 'GPU metrics will not be installed.'
    fi

    if [[ "${ROLE}" != "Master" ]]; then
      configure_gpu_exclusive_mode
    fi
  fi
}

main
