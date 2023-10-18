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

# Fetch Linux Family distro and Dataproc Image version
readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
DATAPROC_IMAGE_VERSION=$(/usr/share/google/get_metadata_value image|grep -Eo 'dataproc-[0-9]-[0-9]'|grep -Eo '[0-9]-[0-9]'|sed -e 's/-/./g')
echo "${DATAPROC_IMAGE_VERSION}" >> /usr/local/share/startup-mig-log

# CUDA version and Driver version config
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.2.2')  #12.2.2
NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '535.104.05') #535.104.05
CUDA_VERSION_MAJOR="${CUDA_VERSION%.*}"  #12.2

SECURE_BOOT="disabled"
SECURE_BOOT=$(mokutil --sb-state|awk '{print $2}')

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

  ## common steps for all linux family distros
  readonly NVIDIA_DRIVER_VERSION_PREFIX=${NVIDIA_DRIVER_VERSION%%.*}

  ## installation steps based OS_NAME
  if [[ ${OS_NAME} == "debian" ]]; then

    DEBIAN_VERSION=$(lsb_release -r|awk '{print $2}') # 10 or 11
    export DEBIAN_FRONTEND=noninteractive

    execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"

    readonly LOCAL_INSTALLER_DEB="cuda-repo-debian${DEBIAN_VERSION}-${CUDA_VERSION_MAJOR//./-}-local_${CUDA_VERSION}-${NVIDIA_DRIVER_VERSION}-1_amd64.deb"
    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "https://developer.download.nvidia.com/compute/cuda/${CUDA_VERSION}/local_installers/${LOCAL_INSTALLER_DEB}" -o /tmp/local-installer.deb

    dpkg -i /tmp/local-installer.deb
    cp /var/cuda-repo-debian${DEBIAN_VERSION}-${CUDA_VERSION_MAJOR//./-}-local/cuda-*-keyring.gpg /usr/share/keyrings/
    add-apt-repository contrib
    execute_with_retries "apt-get update"

    if [[ ${DEBIAN_VERSION} == 10 ]]; then
      apt remove -y libglvnd0
    fi

    execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-${NVIDIA_DRIVER_VERSION_PREFIX}"
    execute_with_retries "apt-get install -y -q --no-install-recommends cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"

    # enable a systemd service that updates kernel headers after reboot
    setup_systemd_update_headers
   
  elif [[ ${OS_NAME} == "ubuntu" ]]; then

    UBUNTU_VERSION=$(lsb_release -r|awk '{print $2}') # 20.04 or 22.04
    UBUNTU_VERSION=${UBUNTU_VERSION%.*} # 20 or 22

    execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"

    readonly UBUNTU_REPO_CUDA_PIN="https://developer.download.nvidia.com/compute/cuda/repos/ubuntu${UBUNTU_VERSION}04/x86_64/cuda-ubuntu${UBUNTU_VERSION}04.pin"
    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${UBUNTU_REPO_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

    readonly LOCAL_INSTALLER_DEB="cuda-repo-ubuntu${UBUNTU_VERSION}04-${CUDA_VERSION_MAJOR//./-}-local_${CUDA_VERSION}-${NVIDIA_DRIVER_VERSION}-1_amd64.deb"
    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "https://developer.download.nvidia.com/compute/cuda/${CUDA_VERSION}/local_installers/${LOCAL_INSTALLER_DEB}" -o /tmp/local-installer.deb

    dpkg -i /tmp/local-installer.deb
    cp /var/cuda-repo-ubuntu${UBUNTU_VERSION}04-${CUDA_VERSION_MAJOR//./-}-local/cuda-*-keyring.gpg /usr/share/keyrings/
    execute_with_retries "apt-get update"    
    
    execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-${NVIDIA_DRIVER_VERSION_PREFIX}"
    execute_with_retries "apt-get install -y -q --no-install-recommends cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"

    # enable a systemd service that updates kernel headers after reboot
    setup_systemd_update_headers

  elif [[ ${OS_NAME} == "rocky" ]]; then

    ROCKY_VERSION=$(lsb_release -r | awk '{print $2}') # 8.8 or 9.1
    ROCKY_VERSION=${ROCKY_VERSION%.*} # 8 or 9

    readonly NVIDIA_ROCKY_REPO_URL="https://developer.download.nvidia.com/compute/cuda/repos/rhel${ROCKY_VERSION}/x86_64/cuda-rhel${ROCKY_VERSION}.repo"
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"
    execute_with_retries "dnf -y -q module install nvidia-driver:${NVIDIA_DRIVER_VERSION_PREFIX}"
    execute_with_retries "dnf -y -q install cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"
    modprobe nvidia

  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
  ldconfig
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
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

# Verify if compatible linux distros and secure boot options are used
function check_os_and_secure_boot() {
  if [[ "${OS_NAME}" == "debian" ]]; then
    DEBIAN_VERSION=$(lsb_release -r | awk '{print $2}') # 10 or 11
    if [[ "${DEBIAN_VERSION}" != "10" && "${DEBIAN_VERSION}" != "11" ]]; then
      echo "Error: The Debian version (${DEBIAN_VERSION}) is not supported. Please use a compatible Debian version."
      exit 1
    fi
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    UBUNTU_VERSION=$(lsb_release -r | awk '{print $2}') # 20.04
    UBUNTU_VERSION=${UBUNTU_VERSION%.*}
    if [[ "${UBUNTU_VERSION}" != "20" && "${UBUNTU_VERSION}" != "22" ]]; then
      echo "Error: The Ubuntu version (${UBUNTU_VERSION}) is not supported. Please use a compatible Ubuntu version."
      exit 1
    fi
  elif [[ "${OS_NAME}" == "rocky" ]]; then
    ROCKY_VERSION=$(lsb_release -r | awk '{print $2}') # 8 or 9
    ROCKY_VERSION=${ROCKY_VERSION%.*}
    if [[ "${ROCKY_VERSION}" != "8" && "${ROCKY_VERSION}" != "9" ]]; then
      echo "Error: The Rocky Linux version (${ROCKY_VERSION}) is not supported. Please use a compatible Rocky Linux version."
      exit 1
    fi
  fi

  if [[ "${SECURE_BOOT}" == "enabled" ]]; then 
    echo "Error: Secure Boot is enabled. Please disable Secure Boot while creating the cluster."
    exit 1
  fi
}

function main() {

  check_os_and_secure_boot
    
  if [[ "${OS_NAME}" == "rocky" ]]; then
    if dnf list kernel-devel-$(uname -r) && dnf list kernel-headers-$(uname -r); then
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
