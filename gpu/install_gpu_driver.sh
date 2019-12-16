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
  local attribute_name=$1
  local default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

readonly GPU_AGENT_REPO_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics'

# Whether to install GPU monitoring agent that sends GPU metrics to StackDriver
INSTALL_GPU_AGENT=$(get_metadata_attribute 'install_gpu_agent' 'false')
readonly INSTALL_GPU_AGENT

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_NAME
OS_DIST=$(lsb_release -cs)
readonly OS_DIST

readonly NVIDIA_DRIVER_VERSION_UBUNTU='435'

function install_gpu_driver() {
  # Detect NVIDIA GPU
  apt-get update
  apt-get install -y pciutils
  if ! (lspci | grep -q NVIDIA); then
    echo 'No NVIDIA card detected. Skipping installation.' >&2
    exit 0
  fi

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
    local nvblas_cpu_blas_lib=/usr/lib/libblas.so
  fi

  if [[ ${OS_NAME} == ubuntu ]]; then
    # Ubuntu-specific Nvidia driver pacakges and modules
    packages+=("nvidia-driver-${NVIDIA_DRIVER_VERSION_UBUNTU}"
      "nvidia-kernel-common-${NVIDIA_DRIVER_VERSION_UBUNTU}")
    modules+=(nvidia)
    local nvblas_cpu_blas_lib=/usr/lib/x86_64-linux-gnu/libblas.so
  fi

  apt-get update
  # Install proprietary NVIDIA Drivers and CUDA
  # See https://wiki.debian.org/NvidiaGraphicsDrivers
  export DEBIAN_FRONTEND=noninteractive
  apt-get install -y "linux-headers-$(uname -r)"
  # Without --no-install-recommends this takes a very long time.
  apt-get install -y -t "${OS_DIST}-backports" --no-install-recommends "${packages[@]}"

  # Create a system wide NVBLAS config
  # See http://docs.nvidia.com/cuda/nvblas/
  local nvblas_config_file=/etc/nvidia/nvblas.conf
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
    systemctl restart hadoop-yarn-nodemanager
  fi

  echo 'NVIDIA GPU driver was installed successfully'
}

# Collect gpu_utilization and gpu_memory_utilization
function install_gpu_agent_service() {
  if ! command -v pip; then
    apt-get install -y python-pip
  fi
  local install_dir=/opt/gpu_utilization_agent
  mkdir "${install_dir}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${GPU_AGENT_REPO_URL}/requirements.txt" -P "${install_dir}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${GPU_AGENT_REPO_URL}/report_gpu_metrics.py" -P "${install_dir}"
  pip install -r "${install_dir}/requirements.txt"

  # Generate GPU service.
  cat <<EOF >/lib/systemd/system/gpu_utilization_agent.service
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
  # Enable gpu_utilization_agent service
  systemctl --now enable gpu_utilization_agent.service
}

function main() {
  # Install GPU NVIDIA Drivers
  install_gpu_driver

  # Install GPU metrics collection in Stackdriver if needed
  if [[ ${INSTALL_GPU_AGENT} == true ]]; then
    install_gpu_agent_service
    echo 'GPU agent successfully deployed.'
  else
    echo 'GPU metrics will not be installed.'
  fi
}

main
