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

function os_id() {
  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs
}

function os_version() {
  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs
}

function is_debian() {
  [[ "$(os_id)" == 'debian' ]]
}

function is_debian10() {
  is_debian && [[ "$(os_version)" == '10'* ]]
}

function is_debian11() {
  is_debian && [[ "$(os_version)" == '11'* ]]
}

function is_debian12() {
  is_debian && [[ "$(os_version)" == '12'* ]]
}

function is_ubuntu() {
  [[ "$(os_id)" == 'ubuntu' ]]
}

function is_ubuntu18() {
  is_ubuntu && [[ "$(os_version)" == '18.04'* ]]
}

function is_ubuntu20() {
  is_ubuntu && [[ "$(os_version)" == '20.04'* ]]
}

function is_ubuntu22() {
  is_ubuntu && [[ "$(os_version)" == '22.04'* ]]
}

function is_rocky() {
  [[ "$(os_id)" == 'rocky' ]]
}

function is_rocky8() {
  is_rocky && [[ "$(os_version)" == '8'* ]]
}

function is_rocky9() {
  is_rocky && [[ "$(os_version)" == '9'* ]]
}

function os_vercat() {
  if is_ubuntu ; then
      os_version | sed -e 's/[^0-9]//g'
  elif is_rocky ; then
      os_version | sed -e 's/[^0-9].*$//g'
  else
      os_version
  fi
}

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value="${2:-}"
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

CA_TMPDIR="$(mktemp -u -d -p /run/tmp -t ca_dir-XXXX)"
PSN="$(get_metadata_attribute private_secret_name)"
readonly PSN
function configure_dkms_certs() {
  if [[ -z "${PSN}" ]]; then
      echo "No signing secret provided.  skipping";
      return 0
  fi

  mkdir -p "${CA_TMPDIR}"

  # If the private key exists, verify it
  if [[ -f "${CA_TMPDIR}/db.rsa" ]]; then
    echo "Private key material exists"

    local expected_modulus_md5sum
    expected_modulus_md5sum=$(get_metadata_attribute cert_modulus_md5sum)
    if [[ -n "${expected_modulus_md5sum}" ]]; then
      modulus_md5sum="${expected_modulus_md5sum}"
    else
      modulus_md5sum="bd40cf5905c7bba4225d330136fdbfd3"
    fi

    # Verify that cert md5sum matches expected md5sum
    if [[ "${modulus_md5sum}" != "$(openssl rsa -noout -modulus -in \"${CA_TMPDIR}/db.rsa\" | openssl md5 | awk '{print $2}')" ]]; then
        echo "unmatched rsa key modulus"
    fi
    ln -sf "${CA_TMPDIR}/db.rsa" /var/lib/dkms/mok.key

    # Verify that key md5sum matches expected md5sum
    if [[ "${modulus_md5sum}" != "$(openssl x509 -noout -modulus -in /var/lib/dkms/mok.pub | openssl md5 | awk '{print $2}')" ]]; then
        echo "unmatched x509 cert modulus"
    fi

    return
  fi


  # Retrieve cloud secrets keys
  local sig_priv_secret_name
  sig_priv_secret_name="${PSN}"
  local sig_pub_secret_name
  sig_pub_secret_name="$(get_metadata_attribute public_secret_name)"
  local sig_secret_project
  sig_secret_project="$(get_metadata_attribute secret_project)"
  local sig_secret_version
  sig_secret_version="$(get_metadata_attribute secret_version)"

  # If metadata values are not set, do not write mok keys
  if [[ -z "${sig_priv_secret_name}" ]]; then return 0 ; fi

  # Write private material to volatile storage
  gcloud secrets versions access "${sig_secret_version}" \
         --project="${sig_secret_project}" \
         --secret="${sig_priv_secret_name}" \
      | dd status=none of="${CA_TMPDIR}/db.rsa"

  # Write public material to volatile storage
  gcloud secrets versions access "${sig_secret_version}" \
         --project="${sig_secret_project}" \
         --secret="${sig_pub_secret_name}" \
      | base64 --decode \
      | dd status=none of="${CA_TMPDIR}/db.der"

  # symlink private key and copy public cert from volatile storage for DKMS
  if is_ubuntu ; then
    mkdir -p /var/lib/shim-signed/mok
    ln -sf "${CA_TMPDIR}/db.rsa" /var/lib/shim-signed/mok/MOK.priv
    cp -f "${CA_TMPDIR}/db.der" /var/lib/shim-signed/mok/MOK.der
  else
    mkdir -p /var/lib/dkms/
    ln -sf "${CA_TMPDIR}/db.rsa" /var/lib/dkms/mok.key
    cp -f "${CA_TMPDIR}/db.der" /var/lib/dkms/mok.pub
  fi
}

function clear_dkms_key {
  if [[ -z "${PSN}" ]]; then
      echo "No signing secret provided.  skipping" >2
      return 0
  fi
  echo "WARN -- PURGING SIGNING MATERIAL -- WARN" >2
  echo "future dkms runs will not use correct signing key" >2
  rm -rf "${CA_TMPDIR}" /var/lib/dkms/mok.key /var/lib/shim-signed/mok/MOK.priv
}

function add_contrib_components() {
  if ! is_debian ; then
    return
  fi
  if is_debian12 ; then
      # Include in sources file components on which nvidia-open-kernel-dkms depends
      local -r debian_sources="/etc/apt/sources.list.d/debian.sources"
      local components="main contrib"

      sed -i -e "s/Components: .*$/Components: ${components}/" "${debian_sources}"
  elif is_debian ; then
      sed -i -e 's/ main$/ main contrib/' /etc/apt/sources.list
  fi
}

# Short name for nvidia urls
if is_rocky ; then
    shortname="$(os_id | sed -e 's/rocky/rhel/')$(os_vercat)"
else
    shortname="$(os_id)$(os_vercat)"
fi
readonly shortname

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

# Fetch Linux Family distro and Dataproc Image version
readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')

# CUDA version and Driver version config
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.4.1')  #12.2.2
NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '550.54.15') #535.104.05
CUDA_VERSION_MAJOR="${CUDA_VERSION%.*}"  #12.2

# EXCEPTIONS
# Change CUDA version for Ubuntu 18 (Cuda 12.1.1 - Driver v530.30.02 is the latest version supported by Ubuntu 18)
if [[ "${OS_NAME}" == "ubuntu" ]]; then
    if is_ubuntu18 ; then
      CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.1.1')  #12.1.1
      NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '530.30.02') #530.30.02
      CUDA_VERSION_MAJOR="${CUDA_VERSION%.*}"  #12.1
    fi
fi

# Verify Secure boot
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

# Enables a systemd service on bootup to install new headers.
# This service recompiles kernel modules for Ubuntu and Debian, which are necessary for the functioning of nvidia-smi.
function setup_systemd_update_headers() {
  cat <<EOF >/lib/systemd/system/install-headers.service
[Unit]
Description=Install Linux headers for the current kernel
After=network-online.target

[Service]
ExecStart=/bin/bash -c 'count=0; while [ \$count -lt 3 ]; do /usr/bin/apt-get install -y -q linux-headers-\$(/bin/uname -r) && break; count=\$((count+1)); sleep 5; done'
Type=oneshot
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
EOF

  # Reload systemd to recognize the new unit file
  systemctl daemon-reload

  # Enable and start the service
  systemctl enable --now install-headers.service
}

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'
readonly NVIDIA_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/${shortname}/x86_64"

# Hold all NVIDIA-related packages from upgrading unintenionally or services like unattended-upgrades
# Users should run apt-mark unhold before they wish to upgrade these packages
function hold_nvidia_packages() {
  apt-mark hold nvidia-*
  apt-mark hold libnvidia-*
  if dpkg -l | grep -q "xserver-xorg-video-nvidia"; then
    apt-mark hold xserver-xorg-video-nvidia*
  fi
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {

  ## common steps for all linux family distros
  readonly NVIDIA_DRIVER_VERSION_PREFIX=${NVIDIA_DRIVER_VERSION%%.*}

  ## For Debian & Ubuntu
  readonly LOCAL_INSTALLER_DEB="cuda-repo-${shortname}-${CUDA_VERSION_MAJOR//./-}-local_${CUDA_VERSION}-${NVIDIA_DRIVER_VERSION}-1_amd64.deb"
  readonly LOCAL_DEB_URL="${NVIDIA_BASE_DL_URL}/cuda/${CUDA_VERSION}/local_installers/${LOCAL_INSTALLER_DEB}"
  readonly DIST_KEYRING_DIR="/var/cuda-repo-${shortname}-${CUDA_VERSION_MAJOR//./-}-local"

  ## installation steps based OS
  if is_debian ; then

    export DEBIAN_FRONTEND=noninteractive

    execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"

    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${LOCAL_DEB_URL}" -o /tmp/local-installer.deb

    dpkg -i /tmp/local-installer.deb
    rm /tmp/local-installer.deb
    cp ${DIST_KEYRING_DIR}/cuda-*-keyring.gpg /usr/share/keyrings/

    add_contrib_components

    execute_with_retries "apt-get update"

    ## EXCEPTION
    if is_debian10 ; then
      apt-get remove -y libglvnd0
      apt-get install -y ca-certificates-java
    fi

    configure_dkms_certs
    execute_with_retries "apt-get install -y -q nvidia-kernel-open-dkms"
    clear_dkms_key
    execute_with_retries \
	"apt-get install -y -q --no-install-recommends cuda-drivers-${NVIDIA_DRIVER_VERSION_PREFIX}"
    execute_with_retries \
	"apt-get install -y -q --no-install-recommends cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"

    modprobe nvidia

    # enable a systemd service that updates kernel headers after reboot
    setup_systemd_update_headers
    # prevent auto upgrading nvidia packages
    hold_nvidia_packages

  elif is_ubuntu ; then

    execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"

    # Ubuntu 18.04 is not supported by new style NV debs; install from .run files + github
    if is_ubuntu18 ; then

      # fetch .run file
      curl -o driver.run \
        "https://download.nvidia.com/XFree86/Linux-x86_64/${NVIDIA_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${NVIDIA_DRIVER_VERSION}.run"
      # Install all but kernel driver
      bash driver.run --no-kernel-modules --silent --install-libglvnd
      rm driver.run

      WORKDIR=/opt/install-nvidia-driver
      mkdir -p "${WORKDIR}"
      pushd $_
      # Fetch open souce kernel module with corresponding tag
      git clone https://github.com/NVIDIA/open-gpu-kernel-modules.git \
          --branch "${NVIDIA_DRIVER_VERSION}" --single-branch
      cd ${WORKDIR}/open-gpu-kernel-modules
      #
      # build kernel modules
      #
      make -j$(nproc) modules \
	   > /var/log/open-gpu-kernel-modules-build.log \
	  2> /var/log/open-gpu-kernel-modules-build_error.log
      configure_dkms_certs
      # sign
      for module in $(find kernel-open -name '*.ko'); do
        /lib/modules/$(uname -r)/build/scripts/sign-file sha256 \
          "${CA_TMPDIR}/db.rsa" \
	  "${CA_TMPDIR}/db.der" \
	  "${module}"
      done
      clear_dkms_key
      # install
      make modules_install \
	   >> /var/log/open-gpu-kernel-modules-build.log \
	  2>> /var/log/open-gpu-kernel-modules-build_error.log
      depmod -a
      modprobe nvidia
      popd

      #
      # Install CUDA
      #
      cuda_runfile="cuda_${CUDA_VERSION}_${NVIDIA_DRIVER_VERSION}_linux.run"
      curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
       "https://developer.download.nvidia.com/compute/cuda/${CUDA_VERSION}/local_installers/${cuda_runfile}" \
       -o cuda.run
      bash cuda.run --silent --toolkit --no-opengl-libs
      rm cuda.run
    else
      # Install from repo provided by NV
      readonly UBUNTU_REPO_CUDA_PIN="${NVIDIA_REPO_URL}/cuda-${shortname}.pin"

      curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
        "${UBUNTU_REPO_CUDA_PIN}" -o /etc/apt/preferences.d/cuda-repository-pin-600

      curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
        "${LOCAL_DEB_URL}" -o /tmp/local-installer.deb

      dpkg -i /tmp/local-installer.deb
      rm /tmp/local-installer.deb
      cp ${DIST_KEYRING_DIR}/cuda-*-keyring.gpg /usr/share/keyrings/
      execute_with_retries "apt-get update"

      execute_with_retries "apt-get install -y -q --no-install-recommends dkms"
      configure_dkms_certs
      for pkg in "nvidia-driver-${NVIDIA_DRIVER_VERSION_PREFIX}-open" \
                 "cuda-drivers-${NVIDIA_DRIVER_VERSION_PREFIX}" \
                 "cuda-toolkit-${CUDA_VERSION_MAJOR//./-}" ; do
        execute_with_retries "apt-get install -y -q --no-install-recommends ${pkg}"
      done
      clear_dkms_key

      modprobe nvidia
    fi


    # enable a systemd service that updates kernel headers after reboot
    setup_systemd_update_headers
    # prevent auto upgrading nvidia packages
    hold_nvidia_packages

  elif is_rocky ; then

    # Ensure the Correct Kernel Development Packages are Installed
    execute_with_retries "dnf -y -q update --exclude=systemd*,kernel*"
    execute_with_retries "dnf -y -q install pciutils kernel-devel gcc"

    readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_REPO_URL}/cuda-${shortname}.repo"
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"
    configure_dkms_certs
    execute_with_retries "dnf -y -q module install nvidia-driver:latest-dkms"
    clear_dkms_key
    execute_with_retries "dnf -y -q install cuda-toolkit"
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

# Verify if compatible linux distros and secure boot options are used
function check_os_and_secure_boot() {
  if is_debian ; then
    if ! is_debian10 && ! is_debian11 && ! is_debian12 ; then
      echo "Error: The Debian version ($(os_version)) is not supported. Please use a compatible Debian version."
      exit 1
    fi
  elif is_ubuntu ; then
    if ! is_ubuntu18 && ! is_ubuntu20 && ! is_ubuntu22 ; then
      echo "Error: The Ubuntu version ($(os_version)) is not supported. Please use a compatible Ubuntu version."
      exit 1
    fi
  elif is_rocky ; then
    if ! is_rocky8 && ! is_rocky9 ; then
      echo "Error: The Rocky Linux version ($(os_version)) is not supported. Please use a compatible Rocky Linux version."
      exit 1
    fi
  fi

  if [[ "${SECURE_BOOT}" == "enabled" && $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    echo "Error: Secure Boot is not supported before image 2.2. Please disable Secure Boot while creating the cluster."
    exit 1
  elif [[ "${SECURE_BOOT}" == "enabled" ]] && [[ -z "${PSN}" ]]; then
      echo "Secure boot is enabled, but no signing material provided."
      echo "Please either disable secure boot or provide signing material as per"
      echo "https://github.com/GoogleCloudDataproc/custom-images/tree/master/examples/secure-boot"
      return 1
  fi
}

function remove_old_backports {
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  oldoldstable=$(curl -s https://deb.debian.org/debian/dists/oldoldstable/Release | awk '/^Codename/ {print $2}');
  oldstable=$(curl -s https://deb.debian.org/debian/dists/oldstable/Release | awk '/^Codename/ {print $2}');
  stable=$(curl -s https://deb.debian.org/debian/dists/stable/Release | awk '/^Codename/ {print $2}');

  matched_files=( $(test -d /etc/apt && grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  if [[ -n "$matched_files" ]]; then
    for filename in "${matched_files[@]}"; do
      # Fetch from archive.debian.org for ${oldoldstable}-backports
      perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                     {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
    done
  fi
}

function main() {
  if [[ ${OS_NAME} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    remove_old_backports
  fi

  check_os_and_secure_boot
    
  if [[ ${OS_NAME} == debian ]] || [[ ${OS_NAME} == ubuntu ]]; then
    export DEBIAN_FRONTEND=noninteractive
    execute_with_retries "apt-get --allow-releaseinfo-change update"
    execute_with_retries "apt-get install -y -q pciutils"
  elif [[ ${OS_NAME} == rocky ]] ; then
    execute_with_retries "dnf -y -q install pciutils"
  else
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
