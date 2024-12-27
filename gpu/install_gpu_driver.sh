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

function os_id()       ( set +x ;  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_version()  ( set +x ;  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_codename() ( set +x ;  grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs ; )

function version_ge() ( set +x ;  [ "$1" = "$(echo -e "$1\n$2" | sort -V | tail -n1)" ] ; )
function version_gt() ( set +x ;  [ "$1" = "$2" ] && return 1 || version_ge $1 $2 ; )
function version_le() ( set +x ;  [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ] ; )
function version_lt() ( set +x ;  [ "$1" = "$2" ] && return 1 || version_le $1 $2 ; )

readonly -A supported_os=(
  ['debian']="10 11 12"
  ['rocky']="8 9"
  ['ubuntu']="18.04 20.04 22.04"
)

# dynamically define OS version test utility functions
if [[ "$(os_id)" == "rocky" ]];
then _os_version=$(os_version | sed -e 's/[^0-9].*$//g')
else _os_version="$(os_version)"; fi
for os_id_val in 'rocky' 'ubuntu' 'debian' ; do
  eval "function is_${os_id_val}() ( set +x ;  [[ \"$(os_id)\" == '${os_id_val}' ]] ; )"

  for osver in $(echo "${supported_os["${os_id_val}"]}") ; do
    eval "function is_${os_id_val}${osver%%.*}() ( set +x ; is_${os_id_val} && [[ \"${_os_version}\" == \"${osver}\" ]] ; )"
    eval "function ge_${os_id_val}${osver%%.*}() ( set +x ; is_${os_id_val} && version_ge \"${_os_version}\" \"${osver}\" ; )"
    eval "function le_${os_id_val}${osver%%.*}() ( set +x ; is_${os_id_val} && version_le \"${_os_version}\" \"${osver}\" ; )"
  done
done

function is_debuntu()  ( set +x ;  is_debian || is_ubuntu ; )

function os_vercat()   ( set +x
  if   is_ubuntu ; then os_version | sed -e 's/[^0-9]//g'
  elif is_rocky  ; then os_version | sed -e 's/[^0-9].*$//g'
                   else os_version ; fi ; )

function repair_old_backports {
  if ge_debian12 || ! is_debuntu ; then return ; fi
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will use archive.debian.org for the oldoldstable repo

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  debdists="https://deb.debian.org/debian/dists"
  oldoldstable=$(curl -s "${debdists}/oldoldstable/Release" | awk '/^Codename/ {print $2}');
  oldstable=$(   curl -s "${debdists}/oldstable/Release"    | awk '/^Codename/ {print $2}');
  stable=$(      curl -s "${debdists}/stable/Release"       | awk '/^Codename/ {print $2}');

  matched_files=( $(test -d /etc/apt && grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  for filename in "${matched_files[@]}"; do
    # Fetch from archive.debian.org for ${oldoldstable}-backports
    perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                  {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
  done
}

function print_metadata_value() {
  local readonly tmpfile=$(mktemp)
  http_code=$(curl -f "${1}" -H "Metadata-Flavor: Google" -w "%{http_code}" \
    -s -o ${tmpfile} 2>/dev/null)
  local readonly return_code=$?
  # If the command completed successfully, print the metadata value to stdout.
  if [[ ${return_code} == 0 && ${http_code} == 200 ]]; then
    cat ${tmpfile}
  fi
  rm -f ${tmpfile}
  return ${return_code}
}

function print_metadata_value_if_exists() {
  local return_code=1
  local readonly url=$1
  print_metadata_value ${url}
  return_code=$?
  return ${return_code}
}

function get_metadata_value() (
  set +x
  local readonly varname=$1
  local -r MDS_PREFIX=http://metadata.google.internal/computeMetadata/v1
  # Print the instance metadata value.
  print_metadata_value_if_exists ${MDS_PREFIX}/instance/${varname}
  return_code=$?
  # If the instance doesn't have the value, try the project.
  if [[ ${return_code} != 0 ]]; then
    print_metadata_value_if_exists ${MDS_PREFIX}/project/${varname}
    return_code=$?
  fi

  return ${return_code}
)

function get_metadata_attribute() (
  set +x
  local -r attribute_name="$1"
  local -r default_value="${2:-}"
  get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
)

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
readonly OS_NAME

# node role
ROLE="$(get_metadata_attribute dataproc-role)"
readonly ROLE

# CUDA version and Driver version
# https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html
# https://developer.nvidia.com/cuda-downloads
# Rocky8: 12.0: 525.147.05
readonly -A DRIVER_FOR_CUDA=(
          ["11.8"]="560.35.03"
          ["12.0"]="525.60.13"  ["12.4"]="560.35.03"  ["12.6"]="560.35.03"
)
# https://developer.nvidia.com/cudnn-downloads
if is_debuntu ; then
readonly -A CUDNN_FOR_CUDA=(
          ["11.8"]="9.5.1.17"
          ["12.0"]="9.5.1.17"   ["12.4"]="9.5.1.17"   ["12.6"]="9.5.1.17"
)
elif is_rocky ; then
# rocky:
#   12.0: 8.8.1.3
#   12.1: 8.9.3.28
#   12.2: 8.9.7.29
#   12.3: 9.0.0.312
#   12.4: 9.1.1.17
#   12.5: 9.2.1.18
#   12.6: 9.5.1.17
readonly -A CUDNN_FOR_CUDA=(
          ["11.8"]="9.5.1.17"
          ["12.0"]="8.8.1.3"   ["12.4"]="9.1.1.17"   ["12.6"]="9.5.1.17"
)
fi
# https://developer.nvidia.com/nccl/nccl-download
# 12.2: 2.19.3, 12.5: 2.21.5
readonly -A NCCL_FOR_CUDA=(
          ["11.8"]="2.15.5"
          ["12.0"]="2.16.5"  ["12.4"]="2.23.4"     ["12.6"]="2.23.4"
)
readonly -A CUDA_SUBVER=(
          ["11.8"]="11.8.0"
          ["12.0"]="12.0.0"  ["12.4"]="12.4.1"     ["12.6"]="12.6.2"
)

RAPIDS_RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
readonly DEFAULT_CUDA_VERSION='12.4'
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' "${DEFAULT_CUDA_VERSION}")
if ( ( ge_debian12 || ge_rocky9 ) && version_le "${CUDA_VERSION%%.*}" "11" ) ; then
  # CUDA 11 no longer supported on debian12 - 2024-11-22, rocky9 - 2024-11-27
  CUDA_VERSION="${DEFAULT_CUDA_VERSION}"
fi

if ( version_ge "${CUDA_VERSION}" "12" && (le_debian11 || le_ubuntu18) ) ; then
  # Only CUDA 12.0 supported on older debuntu
  CUDA_VERSION="12.0"
fi
readonly CUDA_VERSION
readonly CUDA_FULL_VERSION="${CUDA_SUBVER["${CUDA_VERSION}"]}"

function is_cuda12() ( set +x ; [[ "${CUDA_VERSION%%.*}" == "12" ]] ; )
function le_cuda12() ( set +x ; version_le "${CUDA_VERSION%%.*}" "12" ; )
function ge_cuda12() ( set +x ; version_ge "${CUDA_VERSION%%.*}" "12" ; )

function is_cuda11() ( set +x ; [[ "${CUDA_VERSION%%.*}" == "11" ]] ; )
function le_cuda11() ( set +x ; version_le "${CUDA_VERSION%%.*}" "11" ; )
function ge_cuda11() ( set +x ; version_ge "${CUDA_VERSION%%.*}" "11" ; )

DEFAULT_DRIVER="${DRIVER_FOR_CUDA[${CUDA_VERSION}]}"
if ( ge_ubuntu22 && version_le "${CUDA_VERSION}" "12.0" ) ; then
                                         DEFAULT_DRIVER="560.28.03"  ; fi
if ( is_debian11 || is_ubuntu20 ) ; then DEFAULT_DRIVER="560.28.03"  ; fi
if ( is_rocky    && le_cuda11 )   ; then DEFAULT_DRIVER="525.147.05" ; fi
if ( is_ubuntu20 && le_cuda11 )   ; then DEFAULT_DRIVER="535.183.06" ; fi
if ( is_rocky9   && ge_cuda12 )   ; then DEFAULT_DRIVER="565.57.01"  ; fi
DRIVER_VERSION=$(get_metadata_attribute 'gpu-driver-version' "${DEFAULT_DRIVER}")

readonly DRIVER_VERSION
readonly DRIVER=${DRIVER_VERSION%%.*}

readonly DEFAULT_CUDNN8_VERSION="8.0.5.39"
readonly DEFAULT_CUDNN9_VERSION="9.1.0.70"

# Parameters for NVIDIA-provided cuDNN library
readonly DEFAULT_CUDNN_VERSION=${CUDNN_FOR_CUDA["${CUDA_VERSION}"]}
CUDNN_VERSION=$(get_metadata_attribute 'cudnn-version' "${DEFAULT_CUDNN_VERSION}")
function is_cudnn8() ( set +x ; [[ "${CUDNN_VERSION%%.*}" == "8" ]] ; )
function is_cudnn9() ( set +x ; [[ "${CUDNN_VERSION%%.*}" == "9" ]] ; )
# The minimum cuDNN version supported by rocky is ${DEFAULT_CUDNN8_VERSION}
if is_rocky  && (version_le "${CUDNN_VERSION}" "${DEFAULT_CUDNN8_VERSION}") ; then
  CUDNN_VERSION="${DEFAULT_CUDNN8_VERSION}"
elif (ge_ubuntu20 || ge_debian12) && is_cudnn8 ; then
  # cuDNN v8 is not distribution for ubuntu20+, debian12
  CUDNN_VERSION="${DEFAULT_CUDNN9_VERSION}"
elif (le_ubuntu18 || le_debian11) && is_cudnn9 ; then
  # cuDNN v9 is not distributed for ubuntu18, debian10, debian11 ; fall back to 8
  CUDNN_VERSION="8.8.0.121"
fi
readonly CUDNN_VERSION

readonly DEFAULT_NCCL_VERSION=${NCCL_FOR_CUDA["${CUDA_VERSION}"]}
readonly NCCL_VERSION=$(get_metadata_attribute 'nccl-version' ${DEFAULT_NCCL_VERSION})

# Parameters for NVIDIA-provided Debian GPU driver
readonly DEFAULT_USERSPACE_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DRIVER_VERSION}.run"

readonly USERSPACE_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_USERSPACE_URL}")

# Short name for urls
if is_ubuntu22  ; then
    # at the time of writing 20241125 there is no ubuntu2204 in the index of repos at
    # https://developer.download.nvidia.com/compute/machine-learning/repos/
    # use packages from previous release until such time as nvidia
    # release ubuntu2204 builds

    nccl_shortname="ubuntu2004"
    shortname="$(os_id)$(os_vercat)"
elif ge_rocky9 ; then
    # use packages from previous release until such time as nvidia
    # release rhel9 builds

    nccl_shortname="rhel8"
    shortname="rhel9"
elif is_rocky ; then
    shortname="$(os_id | sed -e 's/rocky/rhel/')$(os_vercat)"
    nccl_shortname="${shortname}"
else
    shortname="$(os_id)$(os_vercat)"
    nccl_shortname="${shortname}"
fi

# Parameters for NVIDIA-provided package repositories
readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'
readonly NVIDIA_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/${shortname}/x86_64"

# Parameters for NVIDIA-provided NCCL library
readonly DEFAULT_NCCL_REPO_URL="${NVIDIA_BASE_DL_URL}/machine-learning/repos/${nccl_shortname}/x86_64/nvidia-machine-learning-repo-${nccl_shortname}_1.0.0-1_amd64.deb"
NCCL_REPO_URL=$(get_metadata_attribute 'nccl-repo-url' "${DEFAULT_NCCL_REPO_URL}")
readonly NCCL_REPO_URL
readonly NCCL_REPO_KEY="${NVIDIA_BASE_DL_URL}/machine-learning/repos/${nccl_shortname}/x86_64/7fa2af80.pub" # 3bf863cc.pub

function set_cuda_runfile_url() {
  local RUNFILE_DRIVER_VERSION="${DRIVER_VERSION}"
  local RUNFILE_CUDA_VERSION="${CUDA_FULL_VERSION}"

  if ge_cuda12 ; then
    if ( le_debian11 || le_ubuntu18 ) ; then
      RUNFILE_DRIVER_VERSION="525.60.13"
      RUNFILE_CUDA_VERSION="12.0.0"
    elif ( le_rocky8 && version_le "${DATAPROC_IMAGE_VERSION}" "2.0" ) ; then
      RUNFILE_DRIVER_VERSION="525.147.05"
      RUNFILE_CUDA_VERSION="12.0.0"
    fi
  else
    RUNFILE_DRIVER_VERSION="520.61.05"
    RUNFILE_CUDA_VERSION="11.8.0"
  fi

  readonly RUNFILE_FILENAME="cuda_${RUNFILE_CUDA_VERSION}_${RUNFILE_DRIVER_VERSION}_linux.run"
  CUDA_RELEASE_BASE_URL="${NVIDIA_BASE_DL_URL}/cuda/${RUNFILE_CUDA_VERSION}"
  DEFAULT_NVIDIA_CUDA_URL="${CUDA_RELEASE_BASE_URL}/local_installers/${RUNFILE_FILENAME}"
  readonly DEFAULT_NVIDIA_CUDA_URL

  NVIDIA_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_CUDA_URL}")
  readonly NVIDIA_CUDA_URL
}

set_cuda_runfile_url

# Parameter for NVIDIA-provided Rocky Linux GPU driver
readonly NVIDIA_ROCKY_REPO_URL="${NVIDIA_REPO_URL}/cuda-${shortname}.repo"

CUDNN_TARBALL="cudnn-${CUDA_VERSION}-linux-x64-v${CUDNN_VERSION}.tgz"
CUDNN_TARBALL_URL="${NVIDIA_BASE_DL_URL}/redist/cudnn/v${CUDNN_VERSION%.*}/${CUDNN_TARBALL}"
if ( version_ge "${CUDNN_VERSION}" "8.3.1.22" ); then
  # When version is greater than or equal to 8.3.1.22 but less than 8.4.1.50 use this format
  CUDNN_TARBALL="cudnn-linux-x86_64-${CUDNN_VERSION}_cuda${CUDA_VERSION%.*}-archive.tar.xz"
  if ( version_le "${CUDNN_VERSION}" "8.4.1.50" ); then
    # When cuDNN version is greater than or equal to 8.4.1.50 use this format
    CUDNN_TARBALL="cudnn-linux-x86_64-${CUDNN_VERSION}_cuda${CUDA_VERSION}-archive.tar.xz"
  fi
  # Use legacy url format with one of the tarball name formats depending on version as above
  CUDNN_TARBALL_URL="${NVIDIA_BASE_DL_URL}/redist/cudnn/v${CUDNN_VERSION%.*}/local_installers/${CUDA_VERSION}/${CUDNN_TARBALL}"
fi
if ( version_ge "${CUDA_VERSION}" "12.0" ); then
  # Use modern url format When cuda version is greater than or equal to 12.0
  CUDNN_TARBALL="cudnn-linux-x86_64-${CUDNN_VERSION}_cuda${CUDA_VERSION%%.*}-archive.tar.xz"
  CUDNN_TARBALL_URL="${NVIDIA_BASE_DL_URL}/cudnn/redist/cudnn/linux-x86_64/${CUDNN_TARBALL}"
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

function execute_with_retries() (
  set +x
  local -r cmd="$*"

  if [[ "$cmd" =~ "^apt-get install" ]] ; then
    apt-get -y clean
    apt-get -y autoremove
  fi
  for ((i = 0; i < 3; i++)); do
    set -x
    time eval "$cmd" > "${install_log}" 2>&1 && retval=$? || { retval=$? ; cat "${install_log}" ; }
    set +x
    if [[ $retval == 0 ]] ; then return 0 ; fi
    sleep 5
  done
  return 1
)

CUDA_KEYRING_PKG_INSTALLED="0"
function install_cuda_keyring_pkg() {
  if [[ "${CUDA_KEYRING_PKG_INSTALLED}" == "1" ]]; then return ; fi
  local kr_ver=1.1
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_REPO_URL}/cuda-keyring_${kr_ver}-1_all.deb" \
    -o "${tmpdir}/cuda-keyring.deb"
  dpkg -i "${tmpdir}/cuda-keyring.deb"
  rm -f "${tmpdir}/cuda-keyring.deb"
  CUDA_KEYRING_PKG_INSTALLED="1"
}

function uninstall_cuda_keyring_pkg() {
  apt-get purge -yq cuda-keyring
  CUDA_KEYRING_PKG_INSTALLED="0"
}

CUDA_LOCAL_REPO_INSTALLED="0"
function install_local_cuda_repo() {
  if [[ "${CUDA_LOCAL_REPO_INSTALLED}" == "1" ]]; then return ; fi
  CUDA_LOCAL_REPO_INSTALLED="1"
  pkgname="cuda-repo-${shortname}-${CUDA_VERSION//./-}-local"
  CUDA_LOCAL_REPO_PKG_NAME="${pkgname}"
  readonly LOCAL_INSTALLER_DEB="${pkgname}_${CUDA_FULL_VERSION}-${DRIVER_VERSION}-1_amd64.deb"
  readonly LOCAL_DEB_URL="${NVIDIA_BASE_DL_URL}/cuda/${CUDA_FULL_VERSION}/local_installers/${LOCAL_INSTALLER_DEB}"
  readonly DIST_KEYRING_DIR="/var/${pkgname}"

  curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
    "${LOCAL_DEB_URL}" -o "${tmpdir}/${LOCAL_INSTALLER_DEB}"

  dpkg -i "${tmpdir}/${LOCAL_INSTALLER_DEB}"
  rm "${tmpdir}/${LOCAL_INSTALLER_DEB}"
  cp ${DIST_KEYRING_DIR}/cuda-*-keyring.gpg /usr/share/keyrings/

  if is_ubuntu ; then
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "${NVIDIA_REPO_URL}/cuda-${shortname}.pin" \
      -o /etc/apt/preferences.d/cuda-repository-pin-600
  fi
}
function uninstall_local_cuda_repo(){
  apt-get purge -yq "${CUDA_LOCAL_REPO_PKG_NAME}"
  CUDA_LOCAL_REPO_INSTALLED="0"
}

CUDNN_LOCAL_REPO_INSTALLED="0"
CUDNN_PKG_NAME=""
function install_local_cudnn_repo() {
  if [[ "${CUDNN_LOCAL_REPO_INSTALLED}" == "1" ]]; then return ; fi
  pkgname="cudnn-local-repo-${shortname}-${CUDNN}"
  CUDNN_PKG_NAME="${pkgname}"
  local_deb_fn="${pkgname}_1.0-1_amd64.deb"
  local_deb_url="${NVIDIA_BASE_DL_URL}/cudnn/${CUDNN}/local_installers/${local_deb_fn}"

  # ${NVIDIA_BASE_DL_URL}/redist/cudnn/v8.6.0/local_installers/11.8/cudnn-linux-x86_64-8.6.0.163_cuda11-archive.tar.xz
  curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
    "${local_deb_url}" -o "${tmpdir}/local-installer.deb"

  dpkg -i "${tmpdir}/local-installer.deb"

  rm -f "${tmpdir}/local-installer.deb"

  cp /var/cudnn-local-repo-*-${CUDNN}*/cudnn-local-*-keyring.gpg /usr/share/keyrings

  CUDNN_LOCAL_REPO_INSTALLED="1"
}

function uninstall_local_cudnn_repo() {
  apt-get purge -yq "${CUDNN_PKG_NAME}"
  CUDNN_LOCAL_REPO_INSTALLED="0"
}

CUDNN8_LOCAL_REPO_INSTALLED="0"
CUDNN8_PKG_NAME=""
function install_local_cudnn8_repo() {
  if [[ "${CUDNN8_LOCAL_REPO_INSTALLED}" == "1" ]]; then return ; fi
  if   is_ubuntu ; then cudnn8_shortname="ubuntu2004"
  elif is_debian ; then cudnn8_shortname="debian11"
  else return 0 ; fi
  if   is_cuda12 ; then CUDNN8_CUDA_VER=12.0
  elif is_cuda11 ; then CUDNN8_CUDA_VER=11.8
  else CUDNN8_CUDA_VER="${CUDA_VERSION}" ; fi
  cudnn_pkg_version="${CUDNN_VERSION}-1+cuda${CUDNN8_CUDA_VER}"

  pkgname="cudnn-local-repo-${cudnn8_shortname}-${CUDNN_VERSION}"
  CUDNN8_PKG_NAME="${pkgname}"

  deb_fn="${pkgname}_1.0-1_amd64.deb"
  local_deb_fn="${tmpdir}/${deb_fn}"
  local_deb_url="${NVIDIA_BASE_DL_URL}/redist/cudnn/v${CUDNN}/local_installers/${CUDNN8_CUDA_VER}/${deb_fn}"
  curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
      "${local_deb_url}" -o "${local_deb_fn}"

  dpkg -i "${local_deb_fn}"

  rm -f "${local_deb_fn}"

  cp /var/cudnn-local-repo-*-${CUDNN}*/cudnn-local-*-keyring.gpg /usr/share/keyrings
  CUDNN8_LOCAL_REPO_INSTALLED="1"
}

function uninstall_local_cudnn8_repo() {
  apt-get purge -yq "${CUDNN8_PKG_NAME}"
  CUDNN8_LOCAL_REPO_INSTALLED="0"
}

function install_nvidia_nccl() {
  local -r nccl_version="${NCCL_VERSION}-1+cuda${CUDA_VERSION}"

  if is_rocky ; then
    execute_with_retries \
      dnf -y -q install \
        "libnccl-${nccl_version}" "libnccl-devel-${nccl_version}" "libnccl-static-${nccl_version}"
    sync
  elif is_ubuntu ; then
    install_cuda_keyring_pkg

    apt-get update -qq

    if is_ubuntu18 ; then
      execute_with_retries \
        apt-get install -q -y \
          libnccl2 libnccl-dev
      sync
    else
      execute_with_retries \
        apt-get install -q -y \
          "libnccl2=${nccl_version}" "libnccl-dev=${nccl_version}"
      sync
    fi
  else
    echo "Unsupported OS: '${OS_NAME}'"
    # NB: this tarball is 10GB in size, but can be used to install NCCL on non-ubuntu systems
    # wget https://developer.download.nvidia.com/hpc-sdk/24.7/nvhpc_2024_247_Linux_x86_64_cuda_multi.tar.gz
    # tar xpzf nvhpc_2024_247_Linux_x86_64_cuda_multi.tar.gz
    # nvhpc_2024_247_Linux_x86_64_cuda_multi/install
    return
  fi
}

function is_src_nvidia() ( set +x ; [[ "${GPU_DRIVER_PROVIDER}" == "NVIDIA" ]] ; )
function is_src_os()     ( set +x ; [[ "${GPU_DRIVER_PROVIDER}" == "OS" ]] ; )

function install_nvidia_cudnn() {
  local major_version
  major_version="${CUDNN_VERSION%%.*}"
  local cudnn_pkg_version
  cudnn_pkg_version="${CUDNN_VERSION}-1+cuda${CUDA_VERSION}"

  if is_rocky ; then
    if is_cudnn8 ; then
      execute_with_retries dnf -y -q install \
        "libcudnn${major_version}" \
        "libcudnn${major_version}-devel"
      sync
    elif is_cudnn9 ; then
      execute_with_retries dnf -y -q install \
        "libcudnn9-static-cuda-${CUDA_VERSION%%.*}" \
        "libcudnn9-devel-cuda-${CUDA_VERSION%%.*}"
      sync
    else
      echo "Unsupported cudnn version: '${major_version}'"
    fi
  elif is_debuntu; then
    if ge_debian12 && is_src_os ; then
      apt-get -y install nvidia-cudnn
    else
      local CUDNN="${CUDNN_VERSION%.*}"
      if is_cudnn8 ; then
        install_local_cudnn8_repo

        apt-get update -qq

        execute_with_retries \
          apt-get -y install --no-install-recommends \
            "libcudnn8=${cudnn_pkg_version}" \
            "libcudnn8-dev=${cudnn_pkg_version}"
	sync
      elif is_cudnn9 ; then
	install_cuda_keyring_pkg

        apt-get update -qq

        execute_with_retries \
          apt-get -y install --no-install-recommends \
          "libcudnn9-cuda-${CUDA_VERSION%%.*}" \
          "libcudnn9-dev-cuda-${CUDA_VERSION%%.*}" \
          "libcudnn9-static-cuda-${CUDA_VERSION%%.*}"
	sync
      else
        echo "Unsupported cudnn version: [${CUDNN_VERSION}]"
      fi
    fi
  elif is_ubuntu ; then
    local -a packages
    packages=(
      "libcudnn${major_version}=${cudnn_pkg_version}"
      "libcudnn${major_version}-dev=${cudnn_pkg_version}")
    execute_with_retries \
      apt-get install -q -y --no-install-recommends "${packages[*]}"
    sync
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
  rm -rf "${CA_TMPDIR}" /var/lib/dkms/mok.key /var/lib/shim-signed/mok/MOK.priv
}

function add_contrib_component() {
  if ge_debian12 ; then
      # Include in sources file components on which nvidia-kernel-open-dkms depends
      local -r debian_sources="/etc/apt/sources.list.d/debian.sources"
      local components="main contrib"

      sed -i -e "s/Components: .*$/Components: ${components}/" "${debian_sources}"
  elif is_debian ; then
      sed -i -e 's/ main$/ main contrib/' /etc/apt/sources.list
  fi
}

function add_nonfree_components() {
  if is_src_nvidia ; then return; fi
  if ge_debian12 ; then
      # Include in sources file components on which nvidia-open-kernel-dkms depends
      local -r debian_sources="/etc/apt/sources.list.d/debian.sources"
      local components="main contrib non-free non-free-firmware"

      sed -i -e "s/Components: .*$/Components: ${components}/" "${debian_sources}"
  elif is_debian ; then
      sed -i -e 's/ main$/ main contrib non-free/' /etc/apt/sources.list
  fi
}

function add_repo_nvidia_container_toolkit() {
  if is_debuntu ; then
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

function add_repo_cuda() {
  if is_debuntu ; then
    local kr_path=/usr/share/keyrings/cuda-archive-keyring.gpg
    local sources_list_path="/etc/apt/sources.list.d/cuda-${shortname}-x86_64.list"
    echo "deb [signed-by=${kr_path}] https://developer.download.nvidia.com/compute/cuda/repos/${shortname}/x86_64/ /" \
    | sudo tee "${sources_list_path}"
    curl "${NVIDIA_BASE_DL_URL}/cuda/repos/${shortname}/x86_64/cuda-archive-keyring.gpg" \
      -o "${kr_path}"
  elif is_rocky ; then
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
    execute_with_retries "dnf clean all"
  fi
}

readonly uname_r=$(uname -r)
function build_driver_from_github() {
  if is_ubuntu ; then
    mok_key=/var/lib/shim-signed/mok/MOK.priv
    mok_der=/var/lib/shim-signed/mok/MOK.der
  else
    mok_key=/var/lib/dkms/mok.key
    mok_der=/var/lib/dkms/mok.pub
  fi
  workdir=/opt/install-nvidia-driver
  mkdir -p "${workdir}"
  pushd "${workdir}"
  test -d "${workdir}/open-gpu-kernel-modules" || {
    tarball_fn="${DRIVER_VERSION}.tar.gz"
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "https://github.com/NVIDIA/open-gpu-kernel-modules/archive/refs/tags/${tarball_fn}" \
      | tar xz
    mv "open-gpu-kernel-modules-${DRIVER_VERSION}" open-gpu-kernel-modules
  }
  cd open-gpu-kernel-modules

  time make -j$(nproc) modules \
    >  /var/log/open-gpu-kernel-modules-build.log \
    2> /var/log/open-gpu-kernel-modules-build_error.log
  sync

  if [[ -n "${PSN}" ]]; then
    #configure_dkms_certs
    for module in $(find kernel-open -name '*.ko'); do
      "/lib/modules/${uname_r}/build/scripts/sign-file" sha256 \
      "${mok_key}" \
      "${mok_der}" \
      "${module}"
    done
    #clear_dkms_key
  fi

  make modules_install \
    >> /var/log/open-gpu-kernel-modules-build.log \
    2>> /var/log/open-gpu-kernel-modules-build_error.log
  popd
}

function build_driver_from_packages() {
  if is_debuntu ; then
    if [[ -n "$(apt-cache search -n "nvidia-driver-${DRIVER}-server-open")" ]] ; then
      local pkglist=("nvidia-driver-${DRIVER}-server-open") ; else
      local pkglist=("nvidia-driver-${DRIVER}-open") ; fi
    if is_debian ; then
      pkglist=(
        "firmware-nvidia-gsp=${DRIVER_VERSION}-1"
        "nvidia-smi=${DRIVER_VERSION}-1"
        "nvidia-alternative=${DRIVER_VERSION}-1"
        "nvidia-kernel-open-dkms=${DRIVER_VERSION}-1"
        "nvidia-kernel-support=${DRIVER_VERSION}-1"
        "nvidia-modprobe=${DRIVER_VERSION}-1"
        "libnvidia-ml1=${DRIVER_VERSION}-1"
      )
    fi
    add_contrib_component
    apt-get update -qq
    execute_with_retries apt-get install -y -qq --no-install-recommends dkms
    #configure_dkms_certs
    execute_with_retries apt-get install -y -qq --no-install-recommends "${pkglist[@]}"
    sync

  elif is_rocky ; then
    #configure_dkms_certs
    if execute_with_retries dnf -y -q module install "nvidia-driver:${DRIVER}-dkms" ; then
      echo "nvidia-driver:${DRIVER}-dkms installed successfully"
    else
      execute_with_retries dnf -y -q module install 'nvidia-driver:latest'
    fi
    sync
  fi
  #clear_dkms_key
}

function install_nvidia_userspace_runfile() {
  if test -f "${tmpdir}/userspace-complete" ; then return ; fi
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${USERSPACE_URL}" -o "${tmpdir}/userspace.run"
  execute_with_retries bash "${tmpdir}/userspace.run" --no-kernel-modules --silent --install-libglvnd --tmpdir="${tmpdir}"
  rm -f "${tmpdir}/userspace.run"
  touch "${tmpdir}/userspace-complete"
  sync
}

function install_cuda_runfile() {
  if test -f "${tmpdir}/cuda-complete" ; then return ; fi
  time curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${NVIDIA_CUDA_URL}" -o "${tmpdir}/cuda.run"
  execute_with_retries bash "${tmpdir}/cuda.run" --silent --toolkit --no-opengl-libs --tmpdir="${tmpdir}"
  rm -f "${tmpdir}/cuda.run"
  touch "${tmpdir}/cuda-complete"
  sync
}

function install_cuda_toolkit() {
  local cudatk_package=cuda-toolkit
  if ge_debian12 && is_src_os ; then
    cudatk_package="${cudatk_package}=${CUDA_FULL_VERSION}-1"
  elif [[ -n "${CUDA_VERSION}" ]]; then
    cudatk_package="${cudatk_package}-${CUDA_VERSION//./-}"
  fi
  cuda_package="cuda=${CUDA_FULL_VERSION}-1"
  readonly cudatk_package
  if is_debuntu ; then
#    if is_ubuntu ; then execute_with_retries "apt-get install -y -qq --no-install-recommends cuda-drivers-${DRIVER}=${DRIVER_VERSION}-1" ; fi
    execute_with_retries apt-get install -y -qq --no-install-recommends ${cuda_package} ${cudatk_package}
    sync
  elif is_rocky ; then
    # rocky9: cuda-11-[7,8], cuda-12-[1..6]
    execute_with_retries dnf -y -q install "${cudatk_package}"
    sync
  fi
}

function load_kernel_module() {
  # for some use cases, the kernel module needs to be removed before first use of nvidia-smi
  for module in nvidia_uvm nvidia_drm nvidia_modeset nvidia ; do
    rmmod ${module} > /dev/null 2>&1 || echo "unable to rmmod ${module}"
  done

  depmod -a
  modprobe nvidia
  for suffix in uvm modeset drm; do
    modprobe "nvidia-${suffix}"
  done
  # TODO: if peermem is available, also modprobe nvidia-peermem
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  if ( ge_debian12 && is_src_os ) ; then
    add_nonfree_components
    add_repo_nvidia_container_toolkit
    apt-get update -qq
    #configure_dkms_certs
    apt-get -yq install \
          nvidia-container-toolkit \
          dkms \
          nvidia-open-kernel-dkms \
          nvidia-open-kernel-support \
          nvidia-smi \
          libglvnd0 \
          libcuda1
    #clear_dkms_key
  elif ( le_ubuntu18 || le_debian10 || (ge_debian12 && le_cuda11) ) ; then

    install_nvidia_userspace_runfile

    build_driver_from_github

    install_cuda_runfile
  elif is_debuntu ; then
    install_cuda_keyring_pkg

    build_driver_from_packages

    install_cuda_toolkit
  elif is_rocky ; then
    add_repo_cuda

    build_driver_from_packages

    install_cuda_toolkit
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi
  ldconfig
  if is_src_os ; then
    echo "NVIDIA GPU driver provided by ${OS_NAME} was installed successfully"
  else
    echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
  fi
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  if ! command -v pip; then
    execute_with_retries "apt-get install -y -qq python-pip"
  fi
  local install_dir=/opt/gpu-utilization-agent
  mkdir -p "${install_dir}"
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${GPU_AGENT_REPO_URL}/requirements.txt" -o "${install_dir}/requirements.txt"
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${GPU_AGENT_REPO_URL}/report_gpu_metrics.py" \
    | sed -e 's/-u --format=/--format=/' \
    | dd status=none of="${install_dir}/report_gpu_metrics.py"
  execute_with_retries pip install -r "${install_dir}/requirements.txt"
  sync

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
  "${bdcfg}" set_property \
    --configuration_file "${HADOOP_CONF_DIR}/${config_file}" \
    --name "${property}" --value "${value}" \
    --clobber
}

function configure_yarn() {
  if [[ -d "${HADOOP_CONF_DIR}" && ! -f "${HADOOP_CONF_DIR}/resource-types.xml" ]]; then
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

  if [[ "${#yarn_local_dirs[@]}" -ne "0" && "${yarn_local_dirs[@]}" != "None" ]]; then
    chown yarn:yarn -R "${yarn_local_dirs[@]/,/}"
  fi
}

function configure_gpu_exclusive_mode() {
  # check if running spark 3, if not, enable GPU exclusive mode
  local spark_version
  spark_version=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
  if [[ ${spark_version} != 3.* ]]; then
    # include exclusive mode on GPU
    nvsmi -c EXCLUSIVE_PROCESS
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
  local -r gpus_resources_script="${spark_gpu_script_dir}/getGpusResources.sh"
  cat > "${gpus_resources_script}" <<'EOF'
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

ADDRS=$(nvidia-smi --query-gpu=index --format=csv,noheader | perl -e 'print(join(q{,},map{chomp; qq{"$_"}}<STDIN>))')

echo {\"name\": \"gpu\", \"addresses\":[${ADDRS}]}
EOF

  chmod a+rx "${gpus_resources_script}"

  local spark_defaults_conf="/etc/spark/conf.dist/spark-defaults.conf"
  if ! grep spark.executor.resource.gpu.discoveryScript "${spark_defaults_conf}" ; then
    echo "spark.executor.resource.gpu.discoveryScript=${gpus_resources_script}" >> "${spark_defaults_conf}"
  fi
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

function nvsmi() {
  local nvsmi="/usr/bin/nvidia-smi"
  if   [[ "${nvsmi_works}" == "1" ]] ; then echo "nvidia-smi is working" >&2
  elif [[ ! -f "${nvsmi}" ]]         ; then echo "nvidia-smi not installed" >&2 ; return 0
  elif ! eval "${nvsmi} > /dev/null" ; then echo "nvidia-smi fails" >&2 ; return 0
  else nvsmi_works="1" ; fi

  if [[ "$1" == "-L" ]] ; then
    local NV_SMI_L_CACHE_FILE="/var/run/nvidia-smi_-L.txt"
    if [[ -f "${NV_SMI_L_CACHE_FILE}" ]]; then cat "${NV_SMI_L_CACHE_FILE}"
    else "${nvsmi}" $* | tee "${NV_SMI_L_CACHE_FILE}" ; fi

    return 0
  fi

  "${nvsmi}" $*
}

function install_dependencies() {
  if is_debuntu ; then
    execute_with_retries apt-get install -y -qq pciutils "linux-headers-${uname_r}" screen
  elif is_rocky ; then
    execute_with_retries dnf -y -q install pciutils gcc screen

    local dnf_cmd="dnf -y -q install kernel-devel-${uname_r}"
    local install_log="${tmpdir}/install.log"
    set +e
    eval "${dnf_cmd}" > "${install_log}" 2>&1
    local retval="$?"
    set -e

    if [[ "${retval}" == "0" ]] ; then return ; fi

    if grep -q 'Unable to find a match: kernel-devel-' "${install_log}" ; then
      # this kernel-devel may have been migrated to the vault
      local os_ver="$(echo $uname_r | perl -pe 's/.*el(\d+_\d+)\..*/$1/; s/_/./')"
      local vault="https://download.rockylinux.org/vault/rocky/${os_ver}"
      dnf_cmd="$(echo dnf -y -q --setopt=localpkg_gpgcheck=1 install \
        "${vault}/BaseOS/x86_64/os/Packages/k/kernel-${uname_r}.rpm" \
        "${vault}/BaseOS/x86_64/os/Packages/k/kernel-core-${uname_r}.rpm" \
        "${vault}/BaseOS/x86_64/os/Packages/k/kernel-modules-${uname_r}.rpm" \
        "${vault}/BaseOS/x86_64/os/Packages/k/kernel-modules-core-${uname_r}.rpm" \
        "${vault}/AppStream/x86_64/os/Packages/k/kernel-devel-${uname_r}.rpm"
       )"
    fi

    execute_with_retries "${dnf_cmd}"
  fi
}

function main() {
  # This configuration should be run on all nodes
  # regardless if they have attached GPUs
  configure_yarn

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    # if this is called without the MIG script then the drivers are not installed
    migquery_result="$(nvsmi --query-gpu=mig.mode.current --format=csv,noheader)"
    if [[ "${migquery_result}" == "[N/A]" ]] ; then migquery_result="" ; fi
    NUM_MIG_GPUS="$(echo ${migquery_result} | uniq | wc -l)"

    if [[ "${NUM_MIG_GPUS}" -gt "0" ]] ; then
      if [[ "${NUM_MIG_GPUS}" -eq "1" ]]; then
        if (echo "${migquery_result}" | grep Enabled); then
          IS_MIG_ENABLED=1
          NVIDIA_SMI_PATH='/usr/local/yarn-mig-scripts/'
          MIG_MAJOR_CAPS=`grep nvidia-caps /proc/devices | cut -d ' ' -f 1`
          fetch_mig_scripts
        fi
      fi
    fi

    # if mig is enabled drivers would have already been installed
    if [[ $IS_MIG_ENABLED -eq 0 ]]; then
      install_nvidia_gpu_driver

      load_kernel_module

      if [[ -n ${CUDNN_VERSION} ]]; then
        install_nvidia_nccl
        install_nvidia_cudnn
      fi
      #Install GPU metrics collection in Stackdriver if needed
      if [[ "${INSTALL_GPU_AGENT}" == "true" ]]; then
        install_gpu_agent
        echo 'GPU metrics agent successfully deployed.'
      else
        echo 'GPU metrics agent will not be installed.'
      fi

      # for some use cases, the kernel module needs to be removed before first use of nvidia-smi
      for module in nvidia_uvm nvidia_drm nvidia_modeset nvidia ; do
        rmmod ${module} > /dev/null 2>&1 || echo "unable to rmmod ${module}"
      done

      MIG_GPU_LIST="$(nvsmi -L | grep -e MIG -e P100 -e H100 -e A100 || echo -n "")"
      if test -n "$(nvsmi -L)" ; then
	# cache the result of the gpu query
        ADDRS=$(nvsmi --query-gpu=index --format=csv,noheader | perl -e 'print(join(q{,},map{chomp; qq{"$_"}}<STDIN>))')
        echo "{\"name\": \"gpu\", \"addresses\":[$ADDRS]}" | tee "/var/run/nvidia-gpu-index.txt"
      fi
      NUM_MIG_GPUS="$(test -n "${MIG_GPU_LIST}" && echo "${MIG_GPU_LIST}" | wc -l || echo "0")"
      if [[ "${NUM_MIG_GPUS}" -gt "0" ]] ; then
        # enable MIG on every GPU
	for GPU_ID in $(echo ${MIG_GPU_LIST} | awk -F'[: ]' -e '{print $2}') ; do
	  nvsmi -i "${GPU_ID}" --multi-instance-gpu 1
	done

        NVIDIA_SMI_PATH='/usr/local/yarn-mig-scripts/'
        MIG_MAJOR_CAPS="$(grep nvidia-caps /proc/devices | cut -d ' ' -f 1)"
        fetch_mig_scripts
      else
        configure_gpu_exclusive_mode
      fi
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

function clean_up_sources_lists() {
  #
  # bigtop (primary)
  #
  local -r dataproc_repo_file="/etc/apt/sources.list.d/dataproc.list"

  if [[ -f "${dataproc_repo_file}" ]] && ! grep -q signed-by "${dataproc_repo_file}" ; then
    region="$(get_metadata_value zone | perl -p -e 's:.*/:: ; s:-[a-z]+$::')"

    local regional_bigtop_repo_uri
    regional_bigtop_repo_uri=$(cat ${dataproc_repo_file} |
      sed "s#/dataproc-bigtop-repo/#/goog-dataproc-bigtop-repo-${region}/#" |
      grep "deb .*goog-dataproc-bigtop-repo-${region}.* dataproc contrib" |
      cut -d ' ' -f 2 |
      head -1)

    if [[ "${regional_bigtop_repo_uri}" == */ ]]; then
      local -r bigtop_key_uri="${regional_bigtop_repo_uri}archive.key"
    else
      local -r bigtop_key_uri="${regional_bigtop_repo_uri}/archive.key"
    fi

    local -r bigtop_kr_path="/usr/share/keyrings/bigtop-keyring.gpg"
    rm -f "${bigtop_kr_path}"
    curl -fsS --retry-connrefused --retry 10 --retry-max-time 30 \
      "${bigtop_key_uri}" | gpg --dearmor -o "${bigtop_kr_path}"

    sed -i -e "s:deb https:deb [signed-by=${bigtop_kr_path}] https:g" "${dataproc_repo_file}"
    sed -i -e "s:deb-src https:deb-src [signed-by=${bigtop_kr_path}] https:g" "${dataproc_repo_file}"
  fi

  #
  # adoptium
  #
  # https://adoptium.net/installation/linux/#_deb_installation_on_debian_or_ubuntu
  local -r key_url="https://packages.adoptium.net/artifactory/api/gpg/key/public"
  local -r adoptium_kr_path="/usr/share/keyrings/adoptium.gpg"
  rm -f "${adoptium_kr_path}"
  curl -fsS --retry-connrefused --retry 10 --retry-max-time 30 "${key_url}" \
   | gpg --dearmor -o "${adoptium_kr_path}"
  echo "deb [signed-by=${adoptium_kr_path}] https://packages.adoptium.net/artifactory/deb/ $(os_codename) main" \
   > /etc/apt/sources.list.d/adoptium.list


  #
  # docker
  #
  local docker_kr_path="/usr/share/keyrings/docker-keyring.gpg"
  local docker_repo_file="/etc/apt/sources.list.d/docker.list"
  local -r docker_key_url="https://download.docker.com/linux/$(os_id)/gpg"

  rm -f "${docker_kr_path}"
  curl -fsS --retry-connrefused --retry 10 --retry-max-time 30 "${docker_key_url}" \
    | gpg --dearmor -o "${docker_kr_path}"
  echo "deb [signed-by=${docker_kr_path}] https://download.docker.com/linux/$(os_id) $(os_codename) stable" \
    > ${docker_repo_file}

  #
  # google cloud + logging/monitoring
  #
  if ls /etc/apt/sources.list.d/google-cloud*.list ; then
    rm -f /usr/share/keyrings/cloud.google.gpg
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    for list in google-cloud google-cloud-logging google-cloud-monitoring ; do
      list_file="/etc/apt/sources.list.d/${list}.list"
      if [[ -f "${list_file}" ]]; then
        sed -i -e 's:deb https:deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https:g' "${list_file}"
      fi
    done
  fi

  #
  # cran-r
  #
  if [[ -f /etc/apt/sources.list.d/cran-r.list ]]; then
    keyid="0x95c0faf38db3ccad0c080a7bdc78b2ddeabc47b7"
    if is_ubuntu18 ; then keyid="0x51716619E084DAB9"; fi
    rm -f /usr/share/keyrings/cran-r.gpg
    curl "https://keyserver.ubuntu.com/pks/lookup?op=get&search=${keyid}" | \
      gpg --dearmor -o /usr/share/keyrings/cran-r.gpg
    sed -i -e 's:deb http:deb [signed-by=/usr/share/keyrings/cran-r.gpg] http:g' /etc/apt/sources.list.d/cran-r.list
  fi

  #
  # mysql
  #
  if [[ -f /etc/apt/sources.list.d/mysql.list ]]; then
    rm -f /usr/share/keyrings/mysql.gpg
    curl 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xBCA43417C3B485DD128EC6D4B7B3B788A8D3785C' | \
      gpg --dearmor -o /usr/share/keyrings/mysql.gpg
    sed -i -e 's:deb https:deb [signed-by=/usr/share/keyrings/mysql.gpg] https:g' /etc/apt/sources.list.d/mysql.list
  fi

  if [[ -f /etc/apt/trusted.gpg ]] ; then mv /etc/apt/trusted.gpg /etc/apt/old-trusted.gpg ; fi

}

function exit_handler() {
  set +ex
  echo "Exit handler invoked"

  # Purge private key material until next grant
  clear_dkms_key

  # Clear pip cache
  pip cache purge || echo "unable to purge pip cache"

  # If system memory was sufficient to mount memory-backed filesystems
  if [[ "${tmpdir}" == "/mnt/shm" ]] ; then
    # remove the tmpfs pip cache-dir
    pip config unset global.cache-dir || echo "unable to unset global pip cache"

    # Clean up shared memory mounts
    for shmdir in /var/cache/apt/archives /var/cache/dnf /mnt/shm /tmp ; do
      if grep -q "^tmpfs ${shmdir}" /proc/mounts && ! grep -q "^tmpfs ${shmdir}" /etc/fstab ; then
        umount -f ${shmdir}
      fi
    done

    # restart services stopped during preparation stage
    # systemctl list-units | perl -n -e 'qx(systemctl start $1) if /^.*? ((hadoop|knox|hive|mapred|yarn|hdfs)\S*).service/'
  fi

  if is_debuntu ; then
    # Clean up OS package cache
    apt-get -y -qq clean
    apt-get -y -qq autoremove
    # re-hold systemd package
    if ge_debian12 ; then
    apt-mark hold systemd libsystemd0 ; fi
  else
    dnf clean all
  fi

  # print disk usage statistics for large components
  if is_ubuntu ; then
    du -hs \
      /usr/lib/{pig,hive,hadoop,jvm,spark,google-cloud-sdk,x86_64-linux-gnu} \
      /usr/lib \
      /opt/nvidia/* \
      /usr/local/cuda-1?.? \
      /opt/conda/miniconda3 | sort -h
  elif is_debian ; then
    du -hs \
      /usr/lib/{pig,hive,hadoop,jvm,spark,google-cloud-sdk,x86_64-linux-gnu} \
      /usr/lib \
      /usr/local/cuda-1?.? \
      /opt/conda/miniconda3 | sort -h
  else
    du -hs \
      /var/lib/docker \
      /usr/lib/{pig,hive,hadoop,firmware,jvm,spark,atlas} \
      /usr/lib64/google-cloud-sdk \
      /usr/lib \
      /opt/nvidia/* \
      /usr/local/cuda-1?.? \
      /opt/conda/miniconda3
  fi

  # Process disk usage logs from installation period
  rm -f /run/keep-running-df
  sync
  sleep 5.01s
  # compute maximum size of disk during installation
  # Log file contains logs like the following (minus the preceeding #):
#Filesystem     1K-blocks    Used Available Use% Mounted on
#/dev/vda2        7096908 2611344   4182932  39% /
  df / | tee -a "/run/disk-usage.log"

  perl -e '@siz=( sort { $a => $b }
                   map { (split)[2] =~ /^(\d+)/ }
                  grep { m:^/: } <STDIN> );
$max=$siz[0]; $min=$siz[-1]; $inc=$max-$min;
print( "    samples-taken: ", scalar @siz, $/,
       "maximum-disk-used: $max", $/,
       "minimum-disk-used: $min", $/,
       "     increased-by: $inc", $/ )' < "/run/disk-usage.log"

  echo "exit_handler has completed"

  # zero free disk space
  if [[ -n "$(get_metadata_attribute creating-image)" ]]; then
    dd if=/dev/zero of=/zero
    sync
    sleep 3s
    rm -f /zero
  fi

  return 0
}

function set_proxy(){
  export METADATA_HTTP_PROXY="$(get_metadata_attribute http-proxy)"
  export http_proxy="${METADATA_HTTP_PROXY}"
  export https_proxy="${METADATA_HTTP_PROXY}"
  export HTTP_PROXY="${METADATA_HTTP_PROXY}"
  export HTTPS_PROXY="${METADATA_HTTP_PROXY}"
  export no_proxy=metadata.google.internal,169.254.169.254
  export NO_PROXY=metadata.google.internal,169.254.169.254
}

function mount_ramdisk(){
  local free_mem
  free_mem="$(awk '/^MemFree/ {print $2}' /proc/meminfo)"
  if [[ ${free_mem} -lt 10500000 ]]; then return 0 ; fi

  # Write to a ramdisk instead of churning the persistent disk

  tmpdir="/mnt/shm"
  mkdir -p "${tmpdir}"
  mount -t tmpfs tmpfs "${tmpdir}"

  # Clear pip cache
  # TODO: make this conditional on which OSs have pip without cache purge
  pip cache purge || echo "unable to purge pip cache"

  # Download pip packages to tmpfs
  pip config set global.cache-dir "${tmpdir}" || echo "unable to set global.cache-dir"

  # Download OS packages to tmpfs
  if is_debuntu ; then
    mount -t tmpfs tmpfs /var/cache/apt/archives
  else
    mount -t tmpfs tmpfs /var/cache/dnf
  fi
}

function prepare_to_install(){
  nvsmi_works="0"
  readonly bdcfg="/usr/local/bin/bdconfig"
  tmpdir=/tmp/
  if ! is_debuntu && ! is_rocky ; then
    echo "Unsupported OS: '$(os_name)'"
    exit 1
  fi

  repair_old_backports

  export DEBIAN_FRONTEND=noninteractive

  trap exit_handler EXIT
  mount_ramdisk
  install_log="${tmpdir}/install.log"

  set_proxy

  if is_debuntu ; then
    clean_up_sources_lists
    apt-get update -qq
    apt-get -y clean
    sleep 5s
    apt-get -y -qq autoremove
    if ge_debian12 ; then
    apt-mark unhold systemd libsystemd0 ; fi
  else
    dnf clean all
  fi

  # zero free disk space
  if [[ -n "$(get_metadata_attribute creating-image)" ]]; then ( set +e
    time dd if=/dev/zero of=/zero status=none ; sync ; sleep 3s ; rm -f /zero
  ) fi

  configure_dkms_certs

  install_dependencies

  # Monitor disk usage in a screen session
  df / > "/run/disk-usage.log"
  touch "/run/keep-running-df"
  screen -d -m -US keep-running-df \
    bash -c "while [[ -f /run/keep-running-df ]] ; do df / | tee -a /run/disk-usage.log ; sleep 5s ; done"
}

prepare_to_install

main
