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

# This script installs NVIDIA GPU drivers and enables MIG on Amphere GPU architectures.
# This script should be specified in --metadata=startup-script-url= option and
# --metadata=ENABLE_MIG can be used to enable or disable MIG. The default is to enable it.
# The script does a reboot to fully enable MIG and then configures the MIG device based on the
# user specified MIG_CGI profiles specified via: --metadata=^:^MIG_CGI='9,9'. If MIG_CGI
# is not specified it assumes it's using an A100 and configures 2 instances with profile id 9.
# It is assumed this script is used in conjuntion with install_gpu_driver.sh, which does the
# YARN setup to fully utilize the MIG instances on YARN.
#
# Much of this code is copied from install_gpu_driver.sh to do the driver and CUDA installation.
# It's copied in order to not affect the existing scripts when not using MIG.

set -euxo pipefail

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly OS_NAME

# Dataproc role
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly ROLE

# Parameters for NVIDIA-provided Debian GPU driver
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION='495.29.05'
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION}.run"
NVIDIA_DEBIAN_GPU_DRIVER_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_URL}")
readonly NVIDIA_DEBIAN_GPU_DRIVER_URL

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'

# CUDA Version
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '11.5')
readonly CUDA_VERSION
readonly DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION_PREFIX=${DEFAULT_NVIDIA_DEBIAN_GPU_DRIVER_VERSION%%.*}

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
readonly NVIDIA_UBUNTU_REPO_KEY="${NVIDIA_UBUNTU_REPO_URL}/3bf863cc.pub"
readonly NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu1804.pin"

# Parameter for NVIDIA-provided Rocky Linux GPU driver
readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/rhel8/x86_64/cuda-rhel8.repo"

# Whether to install NVIDIA-provided or OS-provided GPU driver
GPU_DRIVER_PROVIDER=$(get_metadata_attribute 'gpu-driver-provider' 'NVIDIA')
readonly GPU_DRIVER_PROVIDER

# Stackdriver GPU agent parameters
readonly GPU_AGENT_REPO_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics'
# Whether to install GPU monitoring agent that sends GPU metrics to Stackdriver
INSTALL_GPU_AGENT=$(get_metadata_attribute 'install-gpu-agent' 'false')
readonly INSTALL_GPU_AGENT

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

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  if [[ ${OS_NAME} == debian ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_UBUNTU_REPO_KEY}" | apt-key add -
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_DEBIAN_GPU_DRIVER_URL}" -o driver.run
    bash "./driver.run" --silent --install-libglvnd

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_DEBIAN_CUDA_URL}" -o cuda.run
    bash "./cuda.run" --silent --toolkit --no-opengl-libs
  elif [[ ${OS_NAME} == ubuntu ]]; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_UBUNTU_REPO_KEY}" | apt-key add -
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

function enable_mig() {
  nvidia-smi -mig 1
}

function configure_mig_cgi() {
  if (/usr/share/google/get_metadata_value attributes/MIG_CGI); then
    META_MIG_CGI_VALUE=$(/usr/share/google/get_metadata_value attributes/MIG_CGI)
    nvidia-smi mig -cgi $META_MIG_CGI_VALUE -C
  else
    # Dataproc only supports A100's right now split in 2 if not specified
    nvidia-smi mig -cgi 9,9  -C
  fi
}

function main() {
  if [[ ${OS_NAME} != debian ]] && [[ ${OS_NAME} != ubuntu ]] && [[ ${OS_NAME} != rocky ]]; then
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  # default MIG to on when this script is used
  META_MIG_VALUE=1
  if (/usr/share/google/get_metadata_value attributes/ENABLE_MIG); then
    META_MIG_VALUE=$(/usr/share/google/get_metadata_value attributes/ENABLE_MIG)
  fi
  if (lspci | grep -q NVIDIA); then
    if [[ $META_MIG_VALUE -ne 0 ]]; then
      # if the first invocation, the NVIDIA drivers and tools are not installed
      if [[ -f "/usr/bin/nvidia-smi" ]]; then
        # check to see if we already enabled mig mode and rebooted so we don't end
        # up in infinite reboot loop
        NUM_GPUS_WITH_DIFF_MIG_MODES=`/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l`
        if [[ $NUM_GPUS_WITH_DIFF_MIG_MODES -eq 1 ]]; then
          if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | grep Enabled); then
            echo "MIG is enabled on all GPUs, configuring instances"
            configure_mig_cgi
            exit 0
          else
            echo "GPUs present but MIG is not enabled"
          fi
        else
          echo "More than 1 GPU with MIG configured differently between them"
        fi
      fi
    fi
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

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
      execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    fi

    install_nvidia_gpu_driver
    
    # Install GPU metrics collection in Stackdriver if needed
    if [[ ${INSTALL_GPU_AGENT} == true ]]; then
      install_gpu_agent
      echo 'GPU metrics agent successfully deployed.'
    else
      echo 'GPU metrics agent will not be installed.'
    fi

    if [[ META_MIG_VALUE -ne 0 ]]; then
      enable_mig
      NUM_GPUS_WITH_DIFF_MIG_MODES=`/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l`
      if [[ NUM_GPUS_WITH_DIFF_MIG_MODES -eq 1 ]]; then
        if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | grep Enabled); then
          echo "MIG is fully enabled, we don't need to reboot"
          configure_mig_cgi
        else
          echo "MIG is configured on but NOT enabled, we need to reboot"
          reboot
        fi
      else
        echo "MIG is NOT enabled all on GPUs, we need to reboot"
        reboot
      fi
    else
      echo "Not enabling MIG"
    fi
  fi
}

main
