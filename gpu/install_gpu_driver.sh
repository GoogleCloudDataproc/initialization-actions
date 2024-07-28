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

function os_id() {
  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs
}

function os_version() {
  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs
}

function os_codename() {
  grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs
}

function is_rocky() {
  [[ "$(os_id)" == 'rocky' ]]
}

function is_ubuntu() {
  [[ "$(os_id)" == 'ubuntu' ]]
}

function is_ubuntu22() {
  is_ubuntu && [[ "$(os_version)" == '22.04'* ]]
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

function os_vercat() {
  if is_ubuntu ; then
      os_version | sed -e 's/[^0-9]//g'
  elif is_rocky ; then
      os_version | sed -e 's/[^0-9].*$//g'
  else
      os_version
  fi
}


function remove_old_backports {
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  if is_debian12 ; then
    return
  fi

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  oldoldstable=$(curl -s https://deb.debian.org/debian/dists/oldoldstable/Release | awk '/^Codename/ {print $2}');
  oldstable=$(curl -s https://deb.debian.org/debian/dists/oldstable/Release | awk '/^Codename/ {print $2}');
  stable=$(curl -s https://deb.debian.org/debian/dists/stable/Release | awk '/^Codename/ {print $2}');

  matched_files=( $(test -d /etc/apt && grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  for filename in "${matched_files[@]}"; do
    # Fetch from archive.debian.org for ${oldoldstable}-backports
    perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                  {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
  done
}

function compare_versions_lte {
  [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}

function compare_versions_lt() {
  [ "$1" = "$2" ] && return 1 || compare_versions_lte $1 $2
}

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value="${2:-}"
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
readonly OS_NAME

# node role
ROLE="$(get_metadata_attribute dataproc-role)"
readonly ROLE

# CUDA version and Driver version
# https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html
readonly -A DRIVER_FOR_CUDA=( [10.1]="418.88"     [10.2]="440.64.00"
          [11.0]="450.248.02" [11.1]="455.45.01"  [11.2]="460.106.100"
          [11.5]="495.29.05"  [11.6]="510.108.03" [11.7]="515.105.01"
          [11.8]="525.147.05" [12.1]="530.30.02"  [12.4]="550.90.07"
)

readonly -A CUDNN_FOR_CUDA=(  [10.1]="7.6.4.38"   [10.2]="7.6.5.32"
          [11.0]="8.0.4.30"   [11.1]="8.0.5.39"   [11.2]="8.1.1.33"
          [11.5]="8.3.3.40"   [11.6]="8.4.1.50"   [11.7]="8.5.0.96"
          [11.8]="8.6.0.163"  [12.1]="8.9.0"      [12.4]="9.1.0.70"
)
readonly -A NCCL_FOR_CUDA=(   [10.1]="2.4.8"      [10.2]="2.5.6"
          [11.0]="2.7.8"      [11.1]="2.8.3"      [11.2]="2.8.3"
          [11.5]="2.11.4"     [11.6]="2.11.4"     [11.7]="2.12.12"
          [11.8]="2.15.5"     [12.1]="2.17.1"     [12.4]="2.21.5"
)
readonly -A CUDA_SUBVER=(     [10.1]="10.1.243"   [10.2]="10.2.89"
          [11.0]="11.0.3"     [11.1]="11.1.0"     [11.2]="11.2.2"
          [11.5]="11.5.2"     [11.6]="11.6.2"     [11.7]="11.7.1"
          [11.8]="11.8.0"     [12.1]="12.1.0"     [12.4]="12.4.1"
)

RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
DEFAULT_CUDA_VERSION='12.4'
if [[ ${DATAPROC_IMAGE_VERSION} == 2.* ]] && [[ "${RUNTIME}" == "SPARK" ]]; then
  DEFAULT_CUDA_VERSION='12.4'
fi
readonly DEFAULT_CUDA_VERSION
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' "${DEFAULT_CUDA_VERSION}")
readonly CUDA_VERSION
readonly DEFAULT_DRIVER=${DRIVER_FOR_CUDA["${CUDA_VERSION}"]}
readonly DRIVER_VERSION=$(get_metadata_attribute 'gpu-driver-version' ${DEFAULT_DRIVER})
readonly DRIVER=${DRIVER_VERSION%%.*}
# As of Rocky 8.7, kernel 4.18.0-425 is unable to build older nvidia kernel drivers
ROCKY_BINARY_INSTALL="false"
if is_rocky && [[ "${DRIVER}" < "510" ]]; then
  ROCKY_BINARY_INSTALL="true"
fi
readonly ROCKY_BINARY_INSTALL

# Fail early for configurations known to be unsupported
function unsupported_error {
  echo "Unsupported kernel driver on ${distribution}: '${DRIVER}'"
  exit -1
}
if is_rocky ; then
  KERNEL_SUBVERSION=$(uname -r | awk -F- '{print $2}')
  if [[ "${DRIVER}" < "460" && "${DRIVER}" != "450"
     && "${KERNEL_SUBVERSION%%.*}" > "305" ]]; then
    unsupported_error
  fi
elif is_debian ; then
  KERNEL_VERSION=$(uname -r | awk -F- '{print $1}')
  if [[ "${DRIVER}" < "455"
     && $(echo "${KERNEL_VERSION%.*} > 5.7" | bc -l) == 1  ]]; then
    unsupported_error
  fi
fi

DEFAULT_NCCL_VERSION=${NCCL_FOR_CUDA["${CUDA_VERSION}"]}
if is_rocky \
   && (compare_versions_lte "${DEFAULT_NCCL_VERSION}" "2.8.4") ; then
  DEFAULT_NCCL_VERSION="2.8.4"
fi
readonly DEFAULT_NCCL_VERSION
readonly NCCL_VERSION=$(get_metadata_attribute 'nccl-version' ${DEFAULT_NCCL_VERSION})

# Parameters for NVIDIA-provided Debian GPU driver
DEFAULT_DRIVER_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DRIVER_VERSION}.run"
if [[ "$(curl -s -I ${DEFAULT_DRIVER_URL} | head -1 | awk '{print $2}')" != "200" ]]; then
  DEFAULT_DRIVER_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DRIVER}/NVIDIA-Linux-x86_64-${DRIVER}.run"
fi
readonly DEFAULT_DRIVER_URL

DRIVER_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_DRIVER_URL}")
readonly DRIVER_URL

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'

# Short name for urls
nccl_shortname="$(os_id)$(os_vercat)"
if is_ubuntu22  ; then
    # at the time of writing 20240721 there is no ubuntu2204 in the index of repos at
    # https://developer.download.nvidia.com/compute/machine-learning/repos/
    # use packages from previous release until such time as nvidia
    # release ubuntu2204 builds

    nccl_shortname=ubuntu2004
    shortname="$(os_id)$(os_vercat)"
elif is_rocky9 ; then
    # use packages from previous release until such time as nvidia
    # release rhel9 builds

    nccl_shortname="rhel8"
    shortname="rhel9"
elif is_rocky ; then
    shortname="$(os_id | sed -e 's/rocky/rhel/')$(os_vercat)"
else
    shortname="$(os_id)$(os_vercat)"
fi

# Parameters for NVIDIA-provided package repositories
readonly NVIDIA_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/${shortname}/x86_64"

# Parameters for NVIDIA-provided NCCL library
readonly DEFAULT_NCCL_REPO_URL="${NVIDIA_BASE_DL_URL}/machine-learning/repos/${nccl_shortname}/x86_64/nvidia-machine-learning-repo-${nccl_shortname}_1.0.0-1_amd64.deb"
NCCL_REPO_URL=$(get_metadata_attribute 'nccl-repo-url' "${DEFAULT_NCCL_REPO_URL}")
readonly NCCL_REPO_URL
readonly NCCL_REPO_KEY="${NVIDIA_BASE_DL_URL}/machine-learning/repos/${nccl_shortname}/x86_64/7fa2af80.pub"

readonly -A DEFAULT_NVIDIA_CUDA_URLS=(
  [10.1]="${NVIDIA_BASE_DL_URL}/cuda/10.1/Prod/local_installers/cuda_10.1.243_418.87.00_linux.run"
  [10.2]="${NVIDIA_BASE_DL_URL}/cuda/10.2/Prod/local_installers/cuda_10.2.89_440.33.01_linux.run"
  [11.0]="${NVIDIA_BASE_DL_URL}/cuda/11.0.3/local_installers/cuda_11.0.3_450.51.06_linux.run"
  [11.1]="${NVIDIA_BASE_DL_URL}/cuda/11.1.0/local_installers/cuda_11.1.0_455.23.05_linux.run"
  [11.2]="${NVIDIA_BASE_DL_URL}/cuda/11.2.2/local_installers/cuda_11.2.2_460.32.03_linux.run"
  [11.5]="${NVIDIA_BASE_DL_URL}/cuda/11.5.2/local_installers/cuda_11.5.2_495.29.05_linux.run"
  [11.6]="${NVIDIA_BASE_DL_URL}/cuda/11.6.2/local_installers/cuda_11.6.2_510.47.03_linux.run"
  [11.7]="${NVIDIA_BASE_DL_URL}/cuda/11.7.1/local_installers/cuda_11.7.1_515.65.01_linux.run"
  [11.8]="${NVIDIA_BASE_DL_URL}/cuda/11.8.0/local_installers/cuda_11.8.0_520.61.05_linux.run"
  [12.1]="${NVIDIA_BASE_DL_URL}/cuda/12.1.0/local_installers/cuda_12.1.0_530.30.02_linux.run"
  [12.4]="${NVIDIA_BASE_DL_URL}/cuda/12.4.0/local_installers/cuda_12.4.0_550.54.14_linux.run"
)
readonly DEFAULT_NVIDIA_CUDA_URL=${DEFAULT_NVIDIA_CUDA_URLS["${CUDA_VERSION}"]}
NVIDIA_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_CUDA_URL}")
readonly NVIDIA_CUDA_URL


# Parameter for NVIDIA-provided Rocky Linux GPU driver
readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_REPO_URL}/cuda-${shortname}.repo"

# Parameters for NVIDIA-provided CUDNN library
DEFAULT_CUDNN_VERSION=${CUDNN_FOR_CUDA["${CUDA_VERSION}"]}
if is_rocky \
   && (compare_versions_lte "${DEFAULT_CUDNN_VERSION}" "8.0.5.39") ; then
  DEFAULT_CUDNN_VERSION="8.0.5.39"
fi
readonly DEFAULT_CUDNN_VERSION
readonly CUDNN_VERSION=$(get_metadata_attribute 'cudnn-version' "${DEFAULT_CUDNN_VERSION}")
CUDNN_TARBALL="cudnn-${CUDA_VERSION}-linux-x64-v${CUDNN_VERSION}.tgz"
CUDNN_TARBALL_URL="${NVIDIA_BASE_DL_URL}/redist/cudnn/v${CUDNN_VERSION%.*}/${CUDNN_TARBALL}"
if ( compare_versions_lte "8.3.1.22" "${CUDNN_VERSION}" ); then
  CUDNN_TARBALL="cudnn-linux-x86_64-${CUDNN_VERSION}_cuda${CUDA_VERSION%.*}-archive.tar.xz"
  if ( compare_versions_lte "${CUDNN_VERSION}" "8.4.1.50" ); then
    CUDNN_TARBALL="cudnn-linux-x86_64-${CUDNN_VERSION}_cuda${CUDA_VERSION}-archive.tar.xz"
  fi
  CUDNN_TARBALL_URL="${NVIDIA_BASE_DL_URL}/redist/cudnn/v${CUDNN_VERSION%.*}/local_installers/${CUDA_VERSION}/${CUDNN_TARBALL}"
fi
if ( compare_versions_lte "12.0" "${CUDA_VERSION}" ); then
  # When cuda version is greater than 12.0
  CUDNN_TARBALL_URL="${NVIDIA_BASE_DL_URL}/cudnn/redist/cudnn/linux-x86_64/cudnn-linux-x86_64-9.2.0.82_cuda12-archive.tar.xz"
fi
readonly CUDNN_TARBALL
readonly CUDNN_TARBALL_URL

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
  local -r cmd="$*"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_nvidia_nccl() {
  if ( compare_versions_lte "12.0" "${CUDA_VERSION}" ); then
    # When cuda version is greater than 12.0
    echo "no nccl packaged for cuda 12"
    return
  fi

  local -r nccl_version="${NCCL_VERSION}-1+cuda${CUDA_VERSION}"

  if is_rocky ; then
    execute_with_retries "dnf -y -q install libnccl-${nccl_version} libnccl-devel-${nccl_version} libnccl-static-${nccl_version}"
  elif is_ubuntu ; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 "${NCCL_REPO_KEY}" | apt-key add -

    local tmp_dir
    tmp_dir=$(mktemp -d -t gpu-init-action-nccl-XXXX)

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NCCL_REPO_URL}" -o "${tmp_dir}/nvidia-ml-repo.deb"
    dpkg -i "${tmp_dir}/nvidia-ml-repo.deb"

    execute_with_retries "apt-get update"

    execute_with_retries \
      "apt-get install -y " \
        "--allow-unauthenticated libnccl2=${nccl_version} libnccl-dev=${nccl_version}"
  elif is_debian ; then
    echo "nccl not packaged for debian"
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

  if is_rocky ; then
    if is_rocky8 ; then
      execute_with_retries "dnf -y -q install libcudnn8-${cudnn_pkg_version} libcudnn8-devel-${cudnn_pkg_version}"
    elif is_rocky9 ; then
      execute_with_retries "dnf -y -q install" \
      "libcudnn9-static-cuda-${CUDA_VERSION%%.*}" \
      "libcudnn9-devel-cuda-${CUDA_VERSION%%.*}"
    else
      echo "Unsupported OS: '$shortname'"
    fi
  elif is_debian || is_ubuntu; then
    if is_debian12; then
      apt-get -y install nvidia-cudnn
    elif is_debian11; then
      apt-get -y install cudnn9-cuda-12
    else
      local tmp_dir
      tmp_dir=$(mktemp -d -t gpu-init-action-cudnn-XXXX)

      curl -fSsL --retry-connrefused --retry 10 --retry-max-time 30 \
        "${CUDNN_TARBALL_URL}" -o "${tmp_dir}/${CUDNN_TARBALL}"

      if ( compare_versions_lte "${CUDNN_VERSION}" "8.3.0.98" ); then
        tar -xzf "${tmp_dir}/${CUDNN_TARBALL}" -C /usr/local
      else
        ln -sf /usr/local/cuda/targets/x86_64-linux/lib /usr/local/cuda/lib
        tar -h --no-same-owner --strip-components=1 \
          -xJf "${tmp_dir}/${CUDNN_TARBALL}" -C /usr/local/cuda
      fi

      cat <<'EOF' >>/etc/profile.d/cudnn.sh
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:${LD_LIBRARY_PATH}
EOF
    fi
  elif [[ ${OS_NAME} == ubuntu ]]; then
    local -a packages
    packages=(
      "libcudnn${major_version}=${cudnn_pkg_version}"
      "libcudnn${major_version}-dev=${cudnn_pkg_version}")
    execute_with_retries \
      "apt-get install -y --no-install-recommends ${packages[*]}"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  ldconfig

  echo "NVIDIA cuDNN successfully installed for ${OS_NAME}."
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
      echo "No signing secret provided.  skipping" >&2
      return 0
  fi
  echo "WARN -- PURGING SIGNING MATERIAL -- WARN" >&2
  echo "future dkms runs will not use correct signing key" >&2
  rm -rf "${CA_TMPDIR}" /var/lib/dkms/mok.key /var/lib/shim-signed/mok/MOK.priv
}

function add_nonfree_components() {
  if is_debian12 ; then
      # Include in sources file components on which nvidia-open-kernel-dkms depends
      local -r debian_sources="/etc/apt/sources.list.d/debian.sources"
      local components="main contrib non-free non-free-firmware"

      sed -i -e "s/Components: .*$/Components: ${components}/" "${debian_sources}"
  elif is_debian ; then
      sed -i -e 's/ main$/ main contrib non-free/' /etc/apt/sources.list
  fi
}

function add_repo_nvidia_container_toolkit() {
  if is_debian || is_ubuntu ; then
      local kr_path=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg
      local sources_list_path=/etc/apt/sources.list.d/nvidia-container-toolkit.list
      # https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html
      test -f "${kr_path}" ||
        curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
          | gpg --dearmor -o "${kr_path}"

      test -f "${sources_list_path}" ||
	curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list \
          | perl -pe "s#deb https://#deb [signed-by=${kr_path}] https://#g" \
          | tee "${sources_list_path}"
  fi
}


# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  workdir=/opt/install-nvidia-driver
  mkdir -p "${workdir}"
  pushd "${workdir}"

  if is_debian12 ; then
    add_nonfree_components
    add_repo_nvidia_container_toolkit
    apt-get update
    configure_dkms_certs
    apt-get -yq install \
          nvidia-container-toolkit \
          dkms \
          nvidia-open-kernel-dkms \
          nvidia-open-kernel-support \
          nvidia-smi \
          libglvnd0 \
          libcuda1
    clear_dkms_key
    modprobe -r nvidia || echo "no nvidia module loaded"
    modprobe nvidia

  elif is_debian || is_ubuntu ; then
    if is_ubuntu ; then
      mok_key=/var/lib/shim-signed/mok/MOK.priv
      mok_der=/var/lib/shim-signed/mok/MOK.der
    else
      mok_key=/var/lib/dkms/mok.key
      mok_der=/var/lib/dkms/mok.pub
    fi

    # Install CUDA keyring and sources.list
    # https://docs.nvidia.com/cuda/cuda-installation-guide-linux/#network-repo-installation-for-debian
    if is_debian ; then
      PKG_URL="${NVIDIA_REPO_URL}/cuda-keyring_1.1-1_all.deb"
    elif is_ubuntu; then
      PKG_URL="${NVIDIA_REPO_URL}/cuda-keyring_1.0-1_all.deb"
    fi
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${PKG_URL}" -o /tmp/cuda-keyring.deb
    dpkg -i "/tmp/cuda-keyring.deb"
    apt-get update

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${DRIVER_URL}" -o driver.run
    bash "./driver.run" --no-kernel-modules --silent --install-libglvnd
    rm -f driver.run
    git clone https://github.com/NVIDIA/open-gpu-kernel-modules.git --branch "${DRIVER_VERSION}" --single-branch
    pushd open-gpu-kernel-modules
    make -j$(nproc) modules \
      > /var/log/open-gpu-kernel-modules-build.log \
      2> /var/log/open-gpu-kernel-modules-build_error.log

    if [[ -n "${PSN}" ]]; then
      configure_dkms_certs
      for module in $(find kernel-open -name '*.ko'); do
        /lib/modules/$(uname -r)/build/scripts/sign-file sha256 \
        "${mok_key}" \
        "${mok_der}" \
        "${module}"
      done
      clear_dkms_key
    fi
    make modules_install \
      >> /var/log/open-gpu-kernel-modules-build.log \
      2>> /var/log/open-gpu-kernel-modules-build_error.log
    depmod -a
    modprobe -r nvidia || echo "no nvidia module loaded"
    modprobe nvidia
    popd

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_CUDA_URL}" -o cuda.run
    bash "./cuda.run" --silent --toolkit --no-opengl-libs
    rm -f cuda.run
  elif [[ ${OS_NAME} == ubuntu ]]; then # this condition will be met in the previous check ; this code is now skipped
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_REPO_URL}/cuda-keyring_1.0-1_all.deb" \
      -o /tmp/cuda-keyring.deb
    dpkg -i "/tmp/cuda-keyring.deb"

    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_REPO_URL}/cuda-${shortname}.pin" \
      -o /etc/apt/preferences.d/cuda-repository-pin-600

    add-apt-repository "deb ${NVIDIA_REPO_URL} /"
    execute_with_retries "apt-get update"

    if [[ -n "${CUDA_VERSION}" ]]; then
      local -r cuda_package=cuda-toolkit-${CUDA_VERSION//./-}
    else
      local -r cuda_package=cuda-toolkit
    fi
    # Without --no-install-recommends this takes a very long time.
    execute_with_retries "apt-get install -y -q --no-install-recommends cuda-drivers-${DRIVER}"
    execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_package}"
  elif [[ ${OS_NAME} == rocky ]]; then

    # Ensure the Correct Kernel Development Packages are Installed
    execute_with_retries "dnf -y -q update --exclude=systemd*,kernel*"
    execute_with_retries "dnf -y -q install pciutils kernel-devel gcc"

    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"

    configure_dkms_certs
    execute_with_retries "dnf -y -q module install nvidia-driver:${DRIVER}-dkms"
    clear_dkms_key

    execute_with_retries "dnf -y -q install cuda-toolkit-${CUDA_VERSION//./-}"
    modprobe -r nvidia || echo "no nvidia module loaded"
    modprobe nvidia
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
  ldconfig
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
  popd
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  if ! command -v pip; then
    execute_with_retries "apt-get install -y -q python-pip"
  fi
  local install_dir=/opt/gpu-utilization-agent
  mkdir -p "${install_dir}"
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

readonly bdcfg="/usr/local/bin/bdconfig"
function set_hadoop_property() {
  local -r config_file=$1
  local -r property=$2
  local -r value=$3
  "${bdcfg}" set_property \
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
  readarray -d ',' yarn_local_dirs < <("${bdcfg}" get_property_value \
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
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.10/examples/MIG-Support/yarn-unpatched/scripts/nvidia-smi
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.10/examples/MIG-Support/yarn-unpatched/scripts/mig2gpu.sh
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
  if ! is_debian && ! is_ubuntu && ! is_rocky ; then
    echo "Unsupported OS: '$(os_name)'"
    exit 1
  fi

  if is_debian ; then
      remove_old_backports
  fi

  if is_debian || is_ubuntu ; then
    export DEBIAN_FRONTEND=noninteractive
    execute_with_retries "apt-get update"
    execute_with_retries "apt-get install -y -q pciutils"
  elif is_rocky ; then
    execute_with_retries "dnf -y -q update --exclude=systemd*,kernel*"
    execute_with_retries "dnf -y -q install pciutils"
    execute_with_retries "dnf -y -q install kernel-devel"
    execute_with_retries "dnf -y -q install gcc"
  fi

  # This configuration should be run on all nodes
  # regardless if they have attached GPUs
  configure_yarn

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    # if this is called without the MIG script then the drivers are not installed
    nv_smi="/usr/bin/nvidia-smi"
    if (test -f "${nv_smi}" && "${nv_smi}" --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l); then
      NUM_MIG_GPUS=$(${nv_smi} --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l)
      if [[ $NUM_MIG_GPUS -eq 1 ]]; then
        if ("${nv_smi}" --query-gpu=mig.mode.current --format=csv,noheader | grep Enabled); then
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
