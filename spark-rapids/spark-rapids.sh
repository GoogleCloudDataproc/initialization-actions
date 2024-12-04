#!/bin/bash
#
# Copyright 2019,2020,2021,2022,2024 Google LLC and contributors
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

# This script installs NVIDIA GPU drivers (version 535.104.05) along with CUDA 12.2.
# However, Cuda 12.1.1 - Driver v530.30.02 is used for Ubuntu 18 only
# Additionally, it installs the RAPIDS Spark plugin, configures Spark and YARN, and is compatible with Debian, Ubuntu, and Rocky Linux distributions.
# Note that the script is designed to work when secure boot is disabled during cluster creation.
# It also creates a Systemd Service for maintaining up-to-date Kernel Headers on Debian and Ubuntu.

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
if is_rocky ; then shortname="$(os_id | sed -e 's/rocky/rhel/')$(os_vercat)"
              else shortname="$(os_id)$(os_vercat)" ; fi
readonly shortname

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

# Fetch Linux Family distro and Dataproc Image version
OS_NAME="$(lsb_release -is | tr '[:upper:]' '[:lower:]')"
readonly OS_NAME

# Fetch SPARK config
SPARK_VERSION_ENV="$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)"
readonly SPARK_VERSION_ENV
if version_ge "${SPARK_VERSION_ENV}" "3.0" && \
   version_lt "${SPARK_VERSION_ENV}" "4.0" ; then
  readonly DEFAULT_XGBOOST_VERSION="1.7.6" # try 2.1.1
  readonly SPARK_VERSION="3.0"             # try ${SPARK_VERSION_ENV}
else
  echo "Error: Your Spark version is not supported. Please upgrade Spark to one of the supported versions."
  exit 1
fi

# Update SPARK RAPIDS config
readonly DEFAULT_SPARK_RAPIDS_VERSION="24.10.0"
SPARK_RAPIDS_VERSION="$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})"
readonly SPARK_RAPIDS_VERSION
XGBOOST_VERSION="$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})"
readonly XGBOOST_VERSION

# Fetch instance roles and runtime
readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')

# CUDA version and Driver version config

# CUDA version and Driver version
# https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html
# https://developer.nvidia.com/cuda-downloads
# https://docs.nvidia.com/deploy/cuda-compatibility/#id3
readonly -A DRIVER_FOR_CUDA=(
          ["11.8"]="560.35.03"
          ["12.0"]="525.60.13"  ["12.1"]="530.30.02" ["12.4"]="560.35.03"  ["12.6"]="560.35.03"
)
readonly -A CUDA_SUBVER=(
          ["11.8"]="11.8.0"
          ["12.0"]="12.0.0"  ["12.1"]="12.1.0" ["12.4"]="12.4.1" ["12.6"]="12.6.2"
)

CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.6')  # 12.4.1 12.2.2
CUDA_VERSION="$(echo ${CUDA_VERSION} | perl -pe 's/^(\d+\.\d+).*/$1/')" # Strip trailing digit if exists
NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' "${DRIVER_FOR_CUDA["${CUDA_VERSION}"]}") # 550.54.15 535.104.05

# EXCEPTIONS
# Change CUDA version for Ubuntu 18 (Cuda 12.1.1 - Driver v530.30.02 is the latest version supported by Ubuntu 18)
if is_ubuntu18 ; then
  CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.1')  #12.1.1
  CUDA_VERSION="$(echo ${CUDA_VERSION} | perl -pe 's/^(\d+\.\d+).*/$1/')"
  NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '530.30.02') #530.30.02
fi

CUDA_FULL_VERSION="${CUDA_SUBVER["${CUDA_VERSION}"]}"

readonly CUDA_FULL_VERSION
readonly CUDA_VERSION
readonly NVIDIA_DRIVER_VERSION

# Verify Secure boot
SECURE_BOOT="disabled"
SECURE_BOOT=$(mokutil --sb-state|awk '{print $2}')

# Stackdriver GPU agent parameters
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

function install_spark_rapids() {
  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  local -r scala_version="2.12"

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-spark-gpu_${scala_version}/${XGBOOST_VERSION}/xgboost4j-spark-gpu_${scala_version}-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-gpu_${scala_version}/${XGBOOST_VERSION}/xgboost4j-gpu_${scala_version}-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/rapids-4-spark_${scala_version}/${SPARK_RAPIDS_VERSION}/rapids-4-spark_${scala_version}-${SPARK_RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
}

function configure_spark() {
  if version_ge "${SPARK_VERSION}" "3.0" ; then
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when the plan is not finalized,
# query explain output won't show GPU operator, if the user has doubts
# they can uncomment the line before seeing the GPU plan explain;
# having AQE enabled gives user the best performance.
spark.executor.resource.gpu.amount=1
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.dynamicAllocation.enabled=false
spark.sql.autoBroadcastJoinThreshold=10m
spark.sql.files.maxPartitionBytes=512m
# please update this config according to your application
spark.task.resource.gpu.amount=0.25
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  else
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
spark.submit.pyFiles=/usr/lib/spark/jars/xgboost4j-spark_${SPARK_VERSION}-${XGBOOST_VERSION}-${XGBOOST_GPU_SUB_VERSION}.jar
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
  fi
}

# Note that kernel packages are held ; the kernel shipped with a
# dataproc image should be the only kernel it ever runs.  If user
# wants new kernel version, they should deploy new dataproc image
# version.

if is_debuntu ; then
  apt-mark hold linux-image-cloud-amd64
fi

readonly NVIDIA_BASE_DL_URL='https://developer.download.nvidia.com/compute'
readonly NVIDIA_REPO_URL="${NVIDIA_BASE_DL_URL}/cuda/repos/${shortname}/x86_64"

# Hold all NVIDIA-related packages from being upgraded unintenionally
# as part of package management tools such as unattended-upgrades
# Users should run apt-mark unhold before they wish to upgrade these packages
function hold_nvidia_packages() {
  apt-mark hold nvidia-*
  apt-mark hold libnvidia-*
  if dpkg -l | grep -q "xserver-xorg-video-nvidia"; then
    apt-mark hold xserver-xorg-video-nvidia*
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

function install_nvidia_gpu_driver() {
  if ( ge_debian12 && is_src_os ) ; then
    # If source is OS, install from non-nvidia maintainer packages
    add_nonfree_components
    add_repo_nvidia_container_toolkit
    apt-get update -qq
    apt-get -yq install \
          nvidia-container-toolkit \
          dkms \
          nvidia-open-kernel-dkms \
          nvidia-open-kernel-support \
          nvidia-smi \
          libglvnd0 \
          libcuda1

    echo "NVIDIA GPU driver provided by ${OS_NAME} was installed successfully"
    exit 0
  fi

  # Otherwise Install NVIDIA GPU driver provided by NVIDIA
  if ( le_ubuntu18 || le_debian10 || (ge_debian12 && le_cuda11) ) ; then
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
  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  download_agent
  install_agent_dependency
  start_agent_service
}

function download_agent(){
  if ! command -v git; then
    if is_rocky ; then execute_with_retries "dnf -y -q install git"
                  else execute_with_retries "apt-get install -y -qq git" ; fi
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

function set_hadoop_property() {
  local -r config_file=$1
  local -r property=$2
  local -r value=$3
  /usr/local/bin/bdconfig set_property \
    --configuration_file "${HADOOP_CONF_DIR}/${config_file}" \
    --name "${property}" --value "${value}" \
    --clobber
}

function configure_yarn_resources() {
  if [[ ! -d "${HADOOP_CONF_DIR}" ]] ; then return 0 ; fi # pre-init scripts
  if [[ ! -f "${HADOOP_CONF_DIR}/resource-types.xml" ]]; then
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
    nvidia-smi -c EXCLUSIVE_PROCESS
  fi
}

function fetch_mig_scripts() {
  mkdir -p /usr/local/yarn-mig-scripts
  chmod 755 /usr/local/yarn-mig-scripts
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.10/examples/MIG-Support/yarn-unpatched/scripts/nvidia-smi
  wget -P /usr/local/yarn-mig-scripts/ https://raw.githubusercontent.com/NVIDIA/spark-rapids-examples/branch-22.10/examples/MIG-Support/yarn-unpatched/scripts/mig2gpu.sh
  chmod 755 /usr/local/yarn-mig-scripts/*
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

function setup_gpu_yarn() {

  if is_debuntu ; then
    export DEBIAN_FRONTEND=noninteractive
    execute_with_retries "apt-get --allow-releaseinfo-change update"
    execute_with_retries "apt-get install -y -q pciutils"
  elif is_rocky ; then
    execute_with_retries "dnf -y -q install pciutils"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  # This configuration should be run on all nodes
  # regardless if they have attached GPUs
  configure_yarn_resources

  # Detect NVIDIA GPU
  if (lspci | grep -q NVIDIA); then
    # if this is called without the MIG script then the drivers are not installed
    nv_smi="/usr/bin/nvidia-smi"
    if (test -f "${nv_smi}" && "${nv_smi}" --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l); then
      NUM_MIG_GPUS="$($nv_smi --query-gpu=mig.mode.current --format=csv,noheader | uniq | wc -l)"
      if [[ $NUM_MIG_GPUS -eq 1 ]]; then
        if (/usr/bin/nvidia-smi --query-gpu=mig.mode.current --format=csv,noheader | grep Enabled); then
          IS_MIG_ENABLED=1
          NVIDIA_SMI_PATH='/usr/local/yarn-mig-scripts/'
          MIG_MAJOR_CAPS=`grep nvidia-caps /proc/devices | cut -d ' ' -f 1`
          fetch_mig_scripts
        fi
      fi
    fi

    if is_debuntu ; then
      execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    elif is_rocky ; then
      echo "kernel devel and headers not required on rocky.  installing from binary"
    fi

    # if mig is enabled drivers would have already been installed
    if [[ $IS_MIG_ENABLED -eq 0 ]]; then
      install_nvidia_gpu_driver

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
  for svc in resourcemanager nodemanager; do
    if [[ $(systemctl show hadoop-yarn-${svc}.service -p SubState --value) == 'running' ]]; then
      systemctl restart hadoop-yarn-${svc}.service
    fi
  done
}

# Verify if compatible linux distros and secure boot options are used
function check_os_and_secure_boot() {
  if is_debian && ( ! is_debian10 && ! is_debian11 && ! is_debian12 ) ; then
      echo "Error: The Debian version ($(os_version)) is not supported. Please use a compatible Debian version."
      exit 1
  elif is_ubuntu && ( ! is_ubuntu18 && ! is_ubuntu20 && ! is_ubuntu22  ) ; then
      echo "Error: The Ubuntu version ($(os_version)) is not supported. Please use a compatible Ubuntu version."
      exit 1
  elif is_rocky && ( ! is_rocky8 && ! is_rocky9 ) ; then
      echo "Error: The Rocky Linux version ($(os_version)) is not supported. Please use a compatible Rocky Linux version."
      exit 1
  fi

  if [[ "${SECURE_BOOT}" == "enabled" ]] && le_debian11 ; then
    echo "Error: Secure Boot is not supported on Debian before image 2.2. Please disable Secure Boot while creating the cluster."
    exit 1
  elif [[ "${SECURE_BOOT}" == "enabled" ]] && [[ -z "${PSN}" ]]; then
    echo "Secure boot is enabled, but no signing material provided."
    echo "Please either disable secure boot or provide signing material as per"
    echo "https://github.com/GoogleCloudDataproc/custom-images/tree/master/examples/secure-boot"
    return 1
  fi
}

function repair_old_backports {
  if ! is_debuntu ; then return 0 ; fi
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
  repair_old_backports
  check_os_and_secure_boot
  setup_gpu_yarn
  if [[ "${RUNTIME}" == "SPARK" ]]; then
    install_spark_rapids
    configure_spark
    echo "RAPIDS initialized with Spark runtime"
  else
    echo "Unsupported RAPIDS Runtime: ${RUNTIME}"
    exit 1
  fi

  for svc in resourcemanager nodemanager; do
    if [[ $(systemctl show hadoop-yarn-${svc}.service -p SubState --value) == 'running' ]]; then
      systemctl restart hadoop-yarn-${svc}.service
    fi
  done
  if is_debian || is_ubuntu ; then
    apt-get clean
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
#  local key_id="0x45b0f08ff61f64c1c1f61e173f4a517504a9cd61"
  local key_id="0x3b04d753c9050d9a5d343f39843c48a565f8f04b"
#  local -r key_url="https://packages.adoptium.net/artifactory/api/gpg/key/public"
  local -r key_url="https://keyserver.ubuntu.com/pks/lookup?op=get&search=${key_id}"
  local -r adoptium_kr_path="/usr/share/keyrings/adoptium.gpg"
  rm -f "${adoptium_kr_path}"
  curl -fsS --retry-connrefused --retry 10 --retry-max-time 30 "${key_url}" \
   | gpg --no-default-keyring --keyring "${adoptium_kr_path}" --import
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
  METADATA_HTTP_PROXY="$(get_metadata_attribute http-proxy '')"

  if [[ -z "${METADATA_HTTP_PROXY}" ]] ; then return ; fi

  export METADATA_HTTP_PROXY
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

  # Download conda packages to tmpfs
  /opt/conda/miniconda3/bin/conda config --add pkgs_dirs "${tmpdir}"

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
  # Detect dataproc image version from its various names
  if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
    DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
  fi

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
