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

# Download URLs
readonly GPU_AGENT_REPO_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics'

# Whether to install GPU monitoring agent that sends GPU metrics to StackDriver
INSTALL_GPU_AGENT="$(/usr/share/google/get_metadata_value attributes/install_gpu_agent || true)"
readonly INSTALL_GPU_AGENT

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function install_gpu_driver() {
  # Detect NVIDIA GPU
  apt-get update
  apt-get install -y pciutils
  if ! (lspci | grep -q NVIDIA); then
    echo 'No NVIDIA card detected. Skipping installation.' >&2
    exit 0
  fi

  # Add non-free Debian 9 Stretch packages.
  # See https://www.debian.org/distrib/packages#note
  for type in deb deb-src; do
    for distro in stretch stretch-backports; do
      for component in contrib non-free; do
        echo "${type} http://deb.debian.org/debian/ ${distro} ${component}" \
          >>/etc/apt/sources.list.d/non-free.list
      done
    done
  done

  apt-get update
  # Install proprietary NVIDIA Drivers and CUDA
  # See https://wiki.debian.org/NvidiaGraphicsDrivers
  export DEBIAN_FRONTEND=noninteractive
  apt-get install -y "linux-headers-$(uname -r)"
  # Without --no-install-recommends this takes a very long time.
  apt-get install -y -t stretch-backports --no-install-recommends \
    nvidia-cuda-toolkit nvidia-kernel-common nvidia-driver nvidia-smi

  # Create a system wide NVBLAS config
  # See http://docs.nvidia.com/cuda/nvblas/
  NVBLAS_CONFIG_FILE=/etc/nvidia/nvblas.conf
  cat <<EOF >>${NVBLAS_CONFIG_FILE}
# Insert here the CPU BLAS fallback library of your choice.
# The standard libblas.so.3 defaults to OpenBLAS, which does not have the
# requisite CBLAS API.
NVBLAS_CPU_BLAS_LIB /usr/lib/libblas/libblas.so
# Use all GPUs
NVBLAS_GPU_LIST ALL
# Add more configuration here.
EOF
  echo "NVBLAS_CONFIG_FILE=${NVBLAS_CONFIG_FILE}" >>/etc/environment

  # Rebooting during an initialization action is not recommended, so just
  # dynamically load kernel modules. If you want to run an X server, it is
  # recommended that you schedule a reboot to occur after the initialization
  # action finishes.
  modprobe -r nouveau
  modprobe nvidia-current nvidia-drm nvidia-uvm drm

  # Restart any NodeManagers so they pick up the NVBLAS config.
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
  wget -O "${install_dir}/requirements.txt" "${GPU_AGENT_REPO_URL}/requirements.txt"
  wget -O "${install_dir}/report_gpu_metrics.py" "${GPU_AGENT_REPO_URL}/report_gpu_metrics.py"
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

# Install GPU NVIDIA Drivers
install_gpu_driver || err "Installation of Drivers failed"

# Install GPU metrics collection in Stackdriver if needed
if [[ ${INSTALL_GPU_AGENT} == true ]]; then
  install_gpu_agent_service || err "GPU metrics install process failed"
  echo 'GPU agent successfully deployed.'
else
  echo 'GPU metrics will not be installed.'
fi
