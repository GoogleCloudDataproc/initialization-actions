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

## CUDA Version
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
  [11.7]="${NVIDIA_BASE_DL_URL}/cuda/11.7.1/local_installers/cuda_11.7.1_515.65.01_linux.run"
  [11.8]="${NVIDIA_BASE_DL_URL}/cuda/11.8.0/local_installers/cuda_11.8.0_520.61.05_linux.run")
 
readonly DEFAULT_NVIDIA_DEBIAN_CUDA_URL=${DEFAULT_NVIDIA_DEBIAN_CUDA_URLS["${CUDA_VERSION}"]}
NVIDIA_DEBIAN_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_DEBIAN_CUDA_URL}")
readonly NVIDIA_DEBIAN_CUDA_URL
# Parameters for NVIDIA-provided Ubuntu GPU driver
NVIDIA_UBUNTU_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu1804/x86_64"
NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu1804.pin"
readonly NVIDIA_UBUNTU_REPO_KEY_PACKAGE="${NVIDIA_UBUNTU_REPO_URL}/cuda-keyring_1.0-1_all.deb"

SECURE_BOOT="disabled"
SECURE_BOOT=$(mokutil --sb-state|awk '{print $2}')

#echo ${DATAPROC_IMAGE_VERSION}
DATAPROC_IMAGE_VERSION=$(/usr/share/google/get_metadata_value image|grep -Eo 'dataproc-[0-9]-[0-9]'|grep -Eo '[0-9]-[0-9]'|sed -e 's/-/./g')
echo "${DATAPROC_IMAGE_VERSION}" >> /usr/local/share/startup-mig-log

if [[ ${DATAPROC_IMAGE_VERSION} == 2.1 ]]; then
  echo "${DATAPROC_IMAGE_VERSION}" >> /usr/local/share/startup-mig-log
  NVIDIA_UBUNTU_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/ubuntu2004/x86_64"
  NVIDIA_UBUNTU_REPO_CUDA_PIN="${NVIDIA_UBUNTU_REPO_URL}/cuda-ubuntu2004.pin"
fi

# Parameter for NVIDIA-provided Rocky Linux GPU driver
readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/rhel8/x86_64/cuda-rhel8.repo"

# Whether to install NVIDIA-provided or OS-provided GPU driver
GPU_DRIVER_PROVIDER=$(get_metadata_attribute 'gpu-driver-provider' 'NVIDIA')
readonly GPU_DRIVER_PROVIDER

# Stackdriver GPU agent parameters
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
    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
    dpkg -i "/tmp/cuda-keyring.deb"

    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_DEBIAN_GPU_DRIVER_URL}" -o driver.run
    bash "./driver.run" --silent --install-libglvnd

    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_DEBIAN_CUDA_URL}" -o cuda.run
    bash "./cuda.run" --silent --toolkit --no-opengl-libs
  elif [[ ${OS_NAME} == ubuntu ]]; then
    # we need to install additional modules with enabling secure boot, see issue: https://github.com/GoogleCloudDataproc/initialization-actions/issues/1043
    # following [guide](https://cloud.google.com/compute/docs/gpus/install-drivers-gpu#secure-boot) for detailed information.
    if [[ ${SECURE_BOOT} == enabled ]]; then
      NVIDIA_DRIVER_VERSION=$(apt-cache search 'linux-modules-nvidia-[0-9]+-gcp$' | awk '{print $1}' | sort | tail -n 1 | head -n 1 | awk -F"-" '{print $4}')
      apt install linux-modules-nvidia-${NVIDIA_DRIVER_VERSION}-gcp -y
      apt install nvidia-driver-${NVIDIA_DRIVER_VERSION} -y

      echo """
        Package: nsight-compute
        Pin: origin *ubuntu.com*
        Pin-Priority: -1

        Package: nsight-systems
        Pin: origin *ubuntu.com*
        Pin-Priority: -1

        Package: nvidia-modprobe
        Pin: release l=NVIDIA CUDA
        Pin-Priority: 600

        Package: nvidia-settings
        Pin: release l=NVIDIA CUDA
        Pin-Priority: 600

        Package: *
        Pin: release l=NVIDIA CUDA
        Pin-Priority: 100
        """ > /etc/apt/preferences.d/cuda-repository-pin-600

      apt install software-properties-common

      apt-key adv --fetch-keys ${NVIDIA_UBUNTU_REPO_URL}/3bf863cc.pub
      add-apt-repository "deb ${NVIDIA_UBUNTU_REPO_URL} /"

      # CUDA_DRIVER_VERSION should be like "525.60.13-1"
      CUDA_DRIVER_VERSION=$(apt-cache madison cuda-drivers | awk '{print $3}' | sort -r | while read line; do
        if dpkg --compare-versions $(dpkg-query -f='${Version}\n' -W nvidia-driver-${NVIDIA_DRIVER_VERSION}) ge $line ; then
           echo "$line"
           break
        fi
      done)

#      apt-get install -y cuda-drivers-${NVIDIA_DRIVER_VERSION} cuda-drivers=${CUDA_DRIVER_VERSION}
      apt install -y cuda-drivers-${NVIDIA_DRIVER_VERSION}=${CUDA_DRIVER_VERSION} cuda-drivers=${CUDA_DRIVER_VERSION}

      apt-get remove dkms && apt-mark hold dkms

      # the $line should be "cuda-runtime-12-0,cuda-drivers 525.85.12"
      CUDA_VERSION=$(apt-cache showpkg cuda-drivers | grep -o 'cuda-runtime-[0-9][0-9]-[0-9],cuda-drivers [0-9\.]*' | while read line; do
         if dpkg --compare-versions ${CUDA_DRIVER_VERSION} ge $(echo $line | grep -Eo '[[:digit:]]+\.[[:digit:]]+') ; then
             echo $(echo $line | grep -Eo '[[:digit:]]+-[[:digit:]]')
             break
         fi
      done)

      apt install -y cuda-${CUDA_VERSION}

    else
      curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${NVIDIA_UBUNTU_REPO_KEY_PACKAGE}" -o /tmp/cuda-keyring.deb
      dpkg -i "/tmp/cuda-keyring.deb"
      curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
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
    fi
  elif [[ ${OS_NAME} == rocky ]]; then
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"
    # Always install the latest cuda/driver version because old driver version 495 has issues
    execute_with_retries "dnf install -y -q nvidia-driver nvidia-settings cuda-driver"
    modprobe nvidia
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
  ldconfig
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
}


# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  download_agent
  install_agent_dependency
  start_agent_service
}

function download_agent(){
  if [[ ${OS_NAME} == rocky ]]; then
    execute_with_retries "dnf -y -q install git"
  else
    execute_with_retries "apt-get install git -y"
  fi
  mkdir -p /opt/google
  chmod 777 /opt/google
  cd /opt/google
  execute_with_retries "git clone https://github.com/GoogleCloudPlatform/compute-gpu-monitoring.git"
}

function install_agent_dependency(){
  cd /opt/google/compute-gpu-monitoring/linux
  python3 -m venv venv
  venv/bin/pip install wheel
  venv/bin/pip install -Ur requirements.txt
}

function start_agent_service(){
  cp /opt/google/compute-gpu-monitoring/linux/systemd/google_gpu_monitoring_agent_venv.service /lib/systemd/system
  systemctl daemon-reload
  systemctl --no-reload --now enable /lib/systemd/system/google_gpu_monitoring_agent_venv.service
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

function upgrade_kernel() {
  # Determine which kernel is installed
  if [[ "${OS_NAME}" == "debian" ]]; then
    CURRENT_KERNEL_VERSION=`cat /proc/version  | perl -ne 'print( / Debian (\S+) / )'`
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    CURRENT_KERNEL_VERSION=`cat /proc/version | perl -ne 'print( /^Linux version (\S+) / )'`
  elif [[ ${OS_NAME} == rocky ]]; then
    KERN_VER=$(yum info --installed kernel | awk '/^Version/ {print $3}')
    KERN_REL=$(yum info --installed kernel | awk '/^Release/ {print $3}')
    CURRENT_KERNEL_VERSION="${KERN_VER}-${KERN_REL}"
  else
    echo "unsupported OS: ${OS_NAME}!"
    exit -1
  fi

  # Get latest version available in repos
  if [[ "${OS_NAME}" == "debian" ]]; then
    apt-get -qq update
    TARGET_VERSION=$(apt-cache show --no-all-versions linux-image-amd64 | awk '/^Version/ {print $2}')
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    apt-get -qq update
    LATEST_VERSION=$(apt-cache show --no-all-versions linux-image-gcp | awk '/^Version/ {print $2}')
    TARGET_VERSION=`echo ${LATEST_VERSION} | perl -ne 'printf(q{%s-%s-gcp},/(\d+\.\d+\.\d+)\.(\d+)/)'`
  elif [[ "${OS_NAME}" == "rocky" ]]; then
    if yum info --available kernel ; then
      KERN_VER=$(yum info --available kernel | awk '/^Version/ {print $3}')
      KERN_REL=$(yum info --available kernel | awk '/^Release/ {print $3}')
      TARGET_VERSION="${KERN_VER}-${KERN_REL}"
    else
      TARGET_VERSION="${CURRENT_KERNEL_VERSION}"
    fi
  fi

  # Skip this script if we are already on the target version
  if [[ "${CURRENT_KERNEL_VERSION}" == "${TARGET_VERSION}" ]]; then
    echo "target kernel version [${TARGET_VERSION}] is installed"

    # Reboot may have interrupted dpkg.  Bring package system to a good state
    if [[ "${OS_NAME}" == "debian" || "${OS_NAME}" == "ubuntu" ]]; then
      dpkg --configure -a
    fi

    return 0
  fi

  # Install the latest kernel
  if [[ ${OS_NAME} == debian ]]; then
    apt-get install -y linux-image-amd64
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    apt-get install -y linux-image-gcp
  elif [[ "${OS_NAME}" == "rocky" ]]; then
    dnf -y -q install kernel
  fi

  # Make it possible to reboot before init actions are complete - #1033
  DP_ROOT=/usr/local/share/google/dataproc
  STARTUP_SCRIPT="${DP_ROOT}/startup-script.sh"
  POST_HDFS_STARTUP_SCRIPT="${DP_ROOT}/post-hdfs-startup-script.sh"

  for startup_script in ${STARTUP_SCRIPT} ${POST_HDFS_STARTUP_SCRIPT} ; do
    sed -i -e 's:/usr/bin/env bash:/usr/bin/env bash\nexit 0:' ${startup_script}
  done

  cp /var/log/dataproc-initialization-script-0.log /var/log/dataproc-initialization-script-0.log.0

  systemctl reboot
}

function main() {

  if [[ ${OS_NAME} != debian ]] && [[ ${OS_NAME} != ubuntu ]] && [[ ${OS_NAME} != rocky ]]; then
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
    
  if [[ "${OS_NAME}" == "rocky" ]]; then
    if dnf list kernel-devel-$(uname -r) && list kernel-headers-$(uname -r); then
      echo "kernel devel and headers packages are available.  Proceed without kernel upgrade."
    else
      upgrade_kernel
    fi
  fi  
  
  if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
    export DEBIAN_FRONTEND=noninteractive
    execute_with_retries "apt-get update"
    execute_with_retries "apt-get install -y -q pciutils"
  elif [[ ${OS_NAME} == rocky ]] ; then
    execute_with_retries "dnf -y -q install pciutils"
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
  
  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
      execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    elif [[ ${OS_NAME} == rocky ]]; then
      echo "kernel devel and headers not required on rocky.  installing from binary"
    fi

    install_nvidia_gpu_driver

    # Install GPU metrics collection in Stackdriver if needed
    if [[ ${INSTALL_GPU_AGENT} == true ]]; then
      install_gpu_agent
      echo 'GPU metrics agent successfully deployed.'
    else
      echo 'GPU metrics agent will not be installed.'
    fi

    if [[ ${META_MIG_VALUE} -ne 0 ]]; then
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
