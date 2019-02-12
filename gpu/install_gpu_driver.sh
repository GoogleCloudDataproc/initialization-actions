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

set -exo pipefail

# Download settings
readonly REQUIREMENTS_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics/requirements.txt'
readonly REPORT_GPU_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics/report_gpu_metrics.py'


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_gpu(){
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
              >> /etc/apt/sources.list.d/non-free.list
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
    cat <<-EOF >> ${NVBLAS_CONFIG_FILE}
    # Insert here the CPU BLAS fallback library of your choice.
    # The standard libblas.so.3 defaults to OpenBLAS, which does not have the
    # requisite CBLAS API.
    NVBLAS_CPU_BLAS_LIB /usr/lib/libblas/libblas.so
    # Use all GPUs
    NVBLAS_GPU_LIST ALL
    # Add more configuration here.
EOF
    echo "NVBLAS_CONFIG_FILE=${NVBLAS_CONFIG_FILE}" >> /etc/environment

    # Rebooting during an initialization action is not recommended, so just
    # dynamically load kernel modules. If you want to run an X server, it is
    # recommended that you schedule a reboot to occur after the initialization
    # action finishes.
    modprobe -r nouveau
    modprobe nvidia-current
    modprobe nvidia-drm
    modprobe nvidia-uvm
    modprobe drm

    # Restart any NodeManagers so they pick up the NVBLAS config.
    if systemctl status hadoop-yarn-nodemanager; then
      systemctl restart hadoop-yarn-nodemanager
    fi
    echo 'NVIDIA GPU driver was installed successfully'
}

function install_gpu_agent_service(){
    # Collect gpu_utilization and gpu_memory_utilization
    apt-get install python-pip -y
    wget -O report_gpu_metrics.py "${REPORT_GPU_URL}"
    wget -O requirements.txt "${REQUIREMENTS_URL}"
    cp ./requirements.txt /root/requirements.txt
    cp ./report_gpu_metrics.py /root/report_gpu_metrics.py
    pip install -r ./requirements.txt

    # Generate GPU service.
    cat <<-EOF > /lib/systemd/system/gpu_utilization_agent.service
    [Unit]
    Description=GPU Utilization Metric Agent
    [Service]
    Type=simple
    PIDFile=/run/gpu_agent.pid
    ExecStart=/bin/bash --login -c '/usr/bin/python /root/report_gpu_metrics.py'
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
    systemctl --no-reload --now enable /lib/systemd/system/gpu_utilization_agent.service
}


function main() {

  # Determine if GPU services needs to be installed
  local install_gpu_agent
  # Install GPU NVIDIA Drivers
  install_gpu || err "Installation of Drivers failed"
  # Install GPU collection in Stackdriver
  install_gpu_agent="$(/usr/share/google/get_metadata_value attributes/install_gpu_agent || false)"
  if [[ "${install_gpu_agent}" == 'true' ]]; then
    install_gpu_agent_service || err "GPU metrics install process failed"
    echo 'GPU agent successfully deployed.'
  else
    echo 'GPU metrics will not be installed.'
    return 0
  fi
}

main