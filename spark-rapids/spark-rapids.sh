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

# This script installs NVIDIA GPU drivers (version 535.104.05) along with CUDA 12.2.
# However, Cuda 12.1.1 - Driver v530.30.02 is used for Ubuntu 18 only.
# For Ubuntu 24.04 with kernel 6.14+, this script uses repository installation to get the latest CUDA toolkit and NVIDIA driver 570+ for compatibility.
# Additionally, it installs the RAPIDS Spark plugin, configures Spark and YARN, and is compatible with Debian, Ubuntu, and Rocky Linux distributions.
# Note that the script is designed to work when secure boot is disabled during cluster creation.
# It also creates a Systemd Service for maintaining up-to-date Kernel Headers on Debian and Ubuntu.

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

function is_ubuntu24() {
  is_ubuntu && [[ "$(os_version)" == '24.04'* ]]
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

function get_latest_rapids_version() {
  local -r scala_ver=$1
  local -r metadata_url="https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_${scala_ver}/maven-metadata.xml"
  wget -nv -O- "${metadata_url}" 2>/dev/null | sed -n 's/.*<release>\(.*\)<\/release>.*/\1/p'
}

function get_latest_xgboost_version() {
  local -r scala_ver=$1
  local -r metadata_url="https://repo.maven.apache.org/maven2/ml/dmlc/xgboost4j-gpu_${scala_ver}/maven-metadata.xml"
  wget -nv -O- "${metadata_url}" 2>/dev/null | sed -n 's/.*<release>\(.*\)<\/release>.*/\1/p'
}

function get_dkms_cache_path() {
  local -r driver_version=$1
  local -r os_id=$(os_id)
  local -r os_version=$(os_version)
  local -r kernel_version=$(uname -r)
  local -r arch=$(uname -m)
  local -r temp_bucket=$(get_metadata_attribute dataproc-temp-bucket)

  if [[ -z "${temp_bucket}" ]]; then
    echo ""
    return
  fi

  local key_path=""
  if [[ -f "/var/lib/dkms/mok.key" ]]; then
    key_path="/var/lib/dkms/mok.key"
  elif [[ -f "/var/lib/shim-signed/mok/MOK.priv" ]]; then
    key_path="/var/lib/shim-signed/mok/MOK.priv"
  fi

  local key_dir="unsigned"
  if [[ -n "${key_path}" ]]; then
    local md5
    md5=$(openssl rsa -noout -modulus -in "${key_path}" | openssl md5 | awk '{print $2}')
    if [[ -n "${md5}" ]]; then
      key_dir="kmod-${md5}"
    fi
  fi

  echo "gs://${temp_bucket}/nvidia/dkms/${os_id}/${os_version}/${kernel_version}/${arch}/${key_dir}/cuda-${CUDA_VERSION}/nvidia-driver-${driver_version}.tar.gz"
}

function restore_dkms_cache() {
  local -r driver_version=$1
  local -r cache_path=$(get_dkms_cache_path "${driver_version}")

  if [[ -z "${cache_path}" ]]; then
    echo "No temp bucket configured, skipping DKMS cache restore."
    return 1
  fi

  if gsutil -q stat "${cache_path}"; then
    echo "Found cached DKMS module at ${cache_path}. Attempting to restore."
    local -r local_tarball="/tmp/nvidia-dkms-cache.tar.gz"
    gsutil cp "${cache_path}" "${local_tarball}"
    if dkms ldtarball "${local_tarball}"; then
      echo "Successfully loaded DKMS cache."
      rm -f "${local_tarball}"
      return 0
    else
      echo "Failed to load DKMS cache using ldtarball."
      rm -f "${local_tarball}"
      return 1
    fi
  else
    echo "No cached DKMS module found at ${cache_path}."
    return 1
  fi
}

function save_dkms_cache() {
  local -r driver_version=$1
  local -r cache_path=$(get_dkms_cache_path "${driver_version}")

  if [[ -z "${cache_path}" ]]; then
    echo "No temp bucket configured, skipping DKMS cache save."
    return 1
  fi

  # Find the registered nvidia module
  local module_info
  module_info=$(dkms status | grep -i nvidia | head -n1 || true)

  if [[ -z "${module_info}" ]]; then
    echo "No NVIDIA module found in DKMS status. Cannot cache."
    return 1
  fi

  local module_name
  module_name=$(echo "${module_info}" | cut -d, -f1 | cut -d/ -f1)
  local module_version
  module_version=$(echo "${module_info}" | cut -d, -f1 | cut -d/ -f2)

  echo "Caching DKMS module ${module_name}/${module_version} to ${cache_path}"

  local -r local_tarball="/tmp/nvidia-dkms-export.tar.gz"
  rm -f "${local_tarball}"

  if dkms mktarball -m "${module_name}" -v "${module_version}" --archive="${local_tarball}"; then
    gsutil cp "${local_tarball}" "${cache_path}"
    echo "Successfully cached DKMS module."
    if [[ "${cache_path}" == *'kmod-'* ]]; then
      local unsigned_cache_path
      unsigned_cache_path=$(echo "${cache_path}" | sed 's/kmod-[^/]\+/unsigned/')
      if ! gsutil -q stat "${unsigned_cache_path}"; then
        echo "Mirroring signed DKMS cache to ${unsigned_cache_path}"
        gsutil -q cp "${local_tarball}" "${unsigned_cache_path}" || true
      fi
    fi
    rm -f "${local_tarball}"
    return 0
  else
    echo "Failed to create DKMS tarball using --archive. Trying default path."
    if dkms mktarball -m "${module_name}" -v "${module_version}"; then
      local default_tarball
      default_tarball=$(compgen -G "/var/lib/dkms/${module_name}/${module_version}/tarball/*.tar.gz" | head -n1 || true)
      if [[ -n "${default_tarball}" ]]; then
        gsutil cp "${default_tarball}" "${cache_path}"
        echo "Successfully cached DKMS module from default path."
        if [[ "${cache_path}" == *'kmod-'* ]]; then
          local unsigned_cache_path
          unsigned_cache_path=$(echo "${cache_path}" | sed 's/kmod-[^/]\+/unsigned/')
          if ! gsutil -q stat "${unsigned_cache_path}"; then
            echo "Mirroring signed DKMS cache to ${unsigned_cache_path}"
            gsutil -q cp "${default_tarball}" "${unsigned_cache_path}" || true
          fi
        fi
        return 0
      fi
    fi
  fi

  echo "Failed to cache DKMS module."
  return 1
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
    if [[ -z "${expected_modulus_md5sum}" ]]; then
      expected_modulus_md5sum=$(get_metadata_attribute modulus_md5sum)
    fi
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

# Fetch SPARK config
readonly SPARK_VERSION_ENV=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
if [[ "${SPARK_VERSION_ENV}" == "3"* ]]; then
  readonly DEFAULT_XGBOOST_VERSION="1.7.6"
  readonly SPARK_VERSION="3.0"
  readonly SCALA_VERSION="2.12"
elif [[ "${SPARK_VERSION_ENV}" == "4"* ]]; then
  readonly DEFAULT_XGBOOST_VERSION="2.1.4"
  readonly SPARK_VERSION="4.0"
  readonly SCALA_VERSION="2.13"
else
  echo "Error: Your Spark version is not supported. Please upgrade Spark to one of the supported versions."
  exit 1
fi

# Update SPARK RAPIDS config
readonly HARDCODED_RAPIDS_VERSION="26.08.1"

# 1. Try to get explicit version from GCE Metadata
SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' '')
XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' '')

# 2. If not specified, try to auto-detect latest from Maven
if [[ -z "${SPARK_RAPIDS_VERSION}" ]]; then
  echo "INFO: spark-rapids-version not specified in metadata. Attempting to detect latest version..." >&2
  LATEST_RAPIDS=$(get_latest_rapids_version "${SCALA_VERSION}")
  if [[ -n "${LATEST_RAPIDS}" ]]; then
    SPARK_RAPIDS_VERSION="${LATEST_RAPIDS}"
    echo "INFO: Auto-detected latest RAPIDS version: ${SPARK_RAPIDS_VERSION}" >&2
  fi
fi

if [[ -z "${XGBOOST_VERSION}" ]]; then
  echo "INFO: xgboost-version not specified in metadata. Attempting to detect latest version..." >&2
  LATEST_XGBOOST=$(get_latest_xgboost_version "${SCALA_VERSION}")
  if [[ -n "${LATEST_XGBOOST}" ]]; then
    XGBOOST_VERSION="${LATEST_XGBOOST}"
    echo "INFO: Auto-detected latest XGBoost version: ${XGBOOST_VERSION}" >&2
  fi
fi

# 3. If auto-detection failed (e.g. no internet), fall back to hardcoded default
if [[ -z "${SPARK_RAPIDS_VERSION}" ]]; then
  SPARK_RAPIDS_VERSION="${HARDCODED_RAPIDS_VERSION}"
  echo "INFO: Auto-detection failed or skipped. Using hardcoded fallback for RAPIDS: ${SPARK_RAPIDS_VERSION}" >&2
fi

if [[ -z "${XGBOOST_VERSION}" ]]; then
  XGBOOST_VERSION="${DEFAULT_XGBOOST_VERSION}"
  echo "INFO: Auto-detection failed or skipped. Using hardcoded default for XGBoost: ${XGBOOST_VERSION}" >&2
fi

readonly SPARK_RAPIDS_VERSION
readonly XGBOOST_VERSION

# Fetch instance roles and runtime
readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')

# CUDA version and Driver version config
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.4.1')  #12.2.2
NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '550.54.15') #535.104.05
CUDA_VERSION_MAJOR="${CUDA_VERSION%.*}"  #12.2

# EXCEPTIONS
# Debian 12 security kernel 6.1.0-52 includes a four-argument
# pci_resize_resource API that is incompatible with NVIDIA 550 open modules.
if is_debian12 ; then
  NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '580.95.05')
  if [[ "${NVIDIA_DRIVER_VERSION%%.*}" == "550" ]]; then
    echo "WARNING: Driver version 550 is incompatible with Debian 12 kernel. Overriding to 580.95.05" >&2
    NVIDIA_DRIVER_VERSION='580.95.05'
  fi
  USE_REPO_INSTALL="true"
fi

# Change CUDA version for Ubuntu 18 (Cuda 12.1.1 - Driver v530.30.02 is the latest version supported by Ubuntu 18)
# Change CUDA version for Ubuntu 24 (Cuda 12.4.1 is not available, use 12.6.0)
if [[ "${OS_NAME}" == "ubuntu" ]]; then
    if is_ubuntu18 ; then
      CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.1.1')  #12.1.1
      NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '530.30.02') #530.30.02
      CUDA_VERSION_MAJOR="${CUDA_VERSION%.*}"  #12.1
    elif is_ubuntu22 ; then
      # Dataproc 2.2 Ubuntu images can move to newer GCP kernels without
      # changing the image version. Use the online repo to get a compatible
      # package in the selected NVIDIA driver series instead of the older
      # driver embedded in the local CUDA repo.
      USE_REPO_INSTALL="true"
    elif is_ubuntu24 ; then
      # CUDA 12.4.1 is not available for Ubuntu 24.04, use 12.6.0 instead
      # For kernel 6.14+, use NVIDIA driver 570 for compatibility
      KERNEL_VERSION=$(uname -r | cut -d'-' -f1)
      KERNEL_MAJOR=$(echo "$KERNEL_VERSION" | cut -d'.' -f1)
      KERNEL_MINOR=$(echo "$KERNEL_VERSION" | cut -d'.' -f2)

      if [[ "$KERNEL_MAJOR" -eq 6 && "$KERNEL_MINOR" -ge 14 ]]; then
        # For kernel 6.14+ (dataproc 3), use repository installation to get latest CUDA and compatible drivers
        CUDA_VERSION=$(get_metadata_attribute 'cuda-version' 'latest')  #latest from repo
        NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '570') #570 series
        CUDA_VERSION_MAJOR="12"  #Will be determined from repository
        USE_REPO_INSTALL="true"
      else
        # Use CUDA 12.6.0 local installer for older kernels
        CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '12.6.0')  #12.6.0
        NVIDIA_DRIVER_VERSION=$(get_metadata_attribute 'driver-version' '560.28.03') #560.28.03
        CUDA_VERSION_MAJOR="${CUDA_VERSION%.*}"  #12.6
        USE_REPO_INSTALL="false"
      fi
    fi
fi

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

function get_sysfs_gpu_count() {
  local count=0
  for dev in /sys/bus/pci/devices/*; do
    if [[ -f "${dev}/vendor" ]] && [[ "$(cat "${dev}/vendor")" == "0x10de" ]]; then
      if [[ -f "${dev}/class" ]]; then
        local class_code=$(cat "${dev}/class")
        if [[ "${class_code:0:4}" == "0x03" ]]; then
          count=$((count + 1))
        fi
      fi
    fi
  done
  echo $count
}

function execute_with_retries() {
  local -r cmd=$1
  for ((i = 0; i < 10; i++)); do
    if time eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_spark_rapids() {
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  # For Spark 4.0 with Scala 2.13, use the cuda12 variant and Scala 2.13 XGBoost JARs
  if [[ "${SPARK_VERSION}" == "4.0" ]]; then
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_${SCALA_VERSION}/${SPARK_RAPIDS_VERSION}/rapids-4-spark_${SCALA_VERSION}-${SPARK_RAPIDS_VERSION}-cuda12.jar" \
      -P /usr/lib/spark/jars/
    # Download XGBoost JARs for Scala 2.13 (Spark 4.0)
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-spark-gpu_${SCALA_VERSION}/${XGBOOST_VERSION}/xgboost4j-spark-gpu_${SCALA_VERSION}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-gpu_${SCALA_VERSION}/${XGBOOST_VERSION}/xgboost4j-gpu_${SCALA_VERSION}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  else
    # For Spark 3.0 with Scala 2.12
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${nvidia_repo_url}/rapids-4-spark_${SCALA_VERSION}/${SPARK_RAPIDS_VERSION}/rapids-4-spark_${SCALA_VERSION}-${SPARK_RAPIDS_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-spark-gpu_${SCALA_VERSION}/${XGBOOST_VERSION}/xgboost4j-spark-gpu_${SCALA_VERSION}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      "${dmlc_repo_url}/xgboost4j-gpu_${SCALA_VERSION}/${XGBOOST_VERSION}/xgboost4j-gpu_${SCALA_VERSION}-${XGBOOST_VERSION}.jar" \
      -P /usr/lib/spark/jars/
  fi
}

function configure_spark() {
  if [[ "${SPARK_VERSION}" == "3"* ]] || [[ "${SPARK_VERSION}" == "4"* ]]; then
    cat >>${SPARK_CONF_DIR}/spark-defaults.conf <<EOF

###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when the plan is not finalized,
# query explain output won't show GPU operator, if user have doubt
# they can uncomment the line before seeing the GPU plan explain, but AQE on gives user the best performance.
spark.executor.resource.gpu.amount=1
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.resource.gpu.discoveryScript=/usr/lib/spark/scripts/gpu/getGpusResources.sh
spark.dynamicAllocation.enabled=false
spark.sql.autoBroadcastJoinThreshold=10m
spark.sql.files.maxPartitionBytes=512m
# For Spark SQL, we want the scheduler to use the number of CPU cores as the
# limiting resource (the number of tasks we can run in parallel is the number of cores).
# We therefore set the per task GPU amount to a small number, telling the scheduler
# to ignore the GPU when limiting parallel tasks, so we should see "number of cores" tasks
# in parallel able to submit work to the GPU.
spark.task.resource.gpu.amount=0.00001
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
  apt-mark hold nvidia-* > /dev/null 2>&1
  apt-mark hold libnvidia-* > /dev/null 2>&1
  if dpkg -l | grep -q "xserver-xorg-video-nvidia"; then
    apt-mark hold xserver-xorg-video-nvidia* > /dev/null 2>&1
  fi
}

function unhold_nvidia_packages() {
  apt-mark unhold nvidia-*    > /dev/null 2>&1
  apt-mark unhold libnvidia-* > /dev/null 2>&1
  apt-mark unhold xserver-xorg-video-nvidia* > /dev/null 2>&1
}

function configure_ubuntu22_cuda12_compiler() {
  if ! is_ubuntu22 || [[ "${CUDA_VERSION_MAJOR}" != 12* ]]; then
    return
  fi

  # Ubuntu 22 defaults to gcc-11, which fails to build some NVIDIA kernel
  # modules against newer Dataproc 2.2 GCP kernels such as 6.8.0-1058-gcp.
  execute_with_retries "apt-get install -y -q gcc-12 g++-12"
  if [[ -x /usr/bin/gcc-11 ]]; then
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 11
  fi
  update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 12
  update-alternatives --set gcc /usr/bin/gcc-12

  if [[ -x /usr/bin/g++-11 ]]; then
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 11
  fi
  update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 12
  update-alternatives --set g++ /usr/bin/g++-12
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {

  ## common steps for all linux family distros
  readonly NVIDIA_DRIVER_VERSION_PREFIX=${NVIDIA_DRIVER_VERSION%%.*}

  ## For Debian & Ubuntu
  # For driver 570, use the original CUDA installer with driver 560, then upgrade driver separately
  if [[ "${NVIDIA_DRIVER_VERSION_PREFIX}" == "570" ]]; then
    readonly LOCAL_INSTALLER_DEB="cuda-repo-${shortname}-${CUDA_VERSION_MAJOR//./-}-local_${CUDA_VERSION}-560.28.03-1_amd64.deb"
    readonly LOCAL_DEB_URL="${NVIDIA_BASE_DL_URL}/cuda/${CUDA_VERSION}/local_installers/${LOCAL_INSTALLER_DEB}"
  else
    readonly LOCAL_INSTALLER_DEB="cuda-repo-${shortname}-${CUDA_VERSION_MAJOR//./-}-local_${CUDA_VERSION}-${NVIDIA_DRIVER_VERSION}-1_amd64.deb"
    readonly LOCAL_DEB_URL="${NVIDIA_BASE_DL_URL}/cuda/${CUDA_VERSION}/local_installers/${LOCAL_INSTALLER_DEB}"
  fi
  readonly DIST_KEYRING_DIR="/var/cuda-repo-${shortname}-${CUDA_VERSION_MAJOR//./-}-local"

  ## installation steps based OS
  if is_debian ; then

    export DEBIAN_FRONTEND=noninteractive

    execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"

    if [[ "${USE_REPO_INSTALL:-false}" == "true" ]]; then
      execute_with_retries \
        "curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 https://developer.download.nvidia.com/compute/cuda/repos/${shortname}/x86_64/cuda-keyring_1.1-1_all.deb -o /tmp/cuda-keyring_1.1-1_all.deb"
      execute_with_retries "dpkg -i /tmp/cuda-keyring_1.1-1_all.deb"
      rm -f /tmp/cuda-keyring_1.1-1_all.deb
      execute_with_retries "apt-get update"

      execute_with_retries "apt-get install -y -q --no-install-recommends dkms"
      execute_with_retries \
        "apt-get install -y -q --no-install-recommends nvidia-driver-pinning-${NVIDIA_DRIVER_VERSION_PREFIX}"
      execute_with_retries "apt-get update"
      configure_dkms_certs

      local cache_restored=0
      if restore_dkms_cache "${NVIDIA_DRIVER_VERSION}"; then
        cache_restored=1
      fi

      execute_with_retries \
        "apt-get install -y -q --no-install-recommends nvidia-kernel-open-dkms nvidia-driver-cuda"

      if [[ "${cache_restored}" -eq 0 ]]; then
        save_dkms_cache "${NVIDIA_DRIVER_VERSION}" || true
      fi

      clear_dkms_key

      execute_with_retries \
        "apt-get install -y -q --no-install-recommends cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"
    else
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

      if is_debian11 ; then
        apt-get install -y -q --allow-downgrades libglapi-mesa=20.3.5-1 ca-certificates=20210119
      fi

      configure_dkms_certs

      local cache_restored=0
      if restore_dkms_cache "${NVIDIA_DRIVER_VERSION}"; then
        cache_restored=1
      fi

      execute_with_retries "apt-get install -y -q nvidia-kernel-open-dkms"

      if [[ "${cache_restored}" -eq 0 ]]; then
        save_dkms_cache "${NVIDIA_DRIVER_VERSION}" || true
      fi

      clear_dkms_key
      execute_with_retries \
        "apt-get install -y -q --no-install-recommends cuda-drivers-${NVIDIA_DRIVER_VERSION_PREFIX}"
      execute_with_retries \
        "apt-get install -y -q --no-install-recommends cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"
    fi

    modprobe nvidia

    # enable a systemd service that updates kernel headers after reboot
    setup_systemd_update_headers
    # prevent auto upgrading nvidia packages
    hold_nvidia_packages

  elif is_ubuntu ; then

    # Unhold NVIDIA packages to allow upgrades (see issue #1321)
    unhold_nvidia_packages

    configure_ubuntu22_cuda12_compiler

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
      test -d open-gpu-kernel-modules || \
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
      time bash cuda.run --silent --toolkit --no-opengl-libs
      rm cuda.run
    elif [[ "${USE_REPO_INSTALL:-false}" == "true" ]]; then
      # Repository-based installation for latest CUDA and kernel 6.14+ compatibility

      # Install CUDA keyring for repository access
      execute_with_retries "wget https://developer.download.nvidia.com/compute/cuda/repos/${shortname}/x86_64/cuda-keyring_1.1-1_all.deb"
      execute_with_retries "dpkg -i cuda-keyring_1.1-1_all.deb"
      rm -f cuda-keyring_1.1-1_all.deb

      # Add graphics-drivers PPA for latest NVIDIA drivers
      execute_with_retries "apt-get install -y -q software-properties-common"
      execute_with_retries "add-apt-repository -y ppa:graphics-drivers/ppa"
      execute_with_retries "apt-get update"

      execute_with_retries "apt-get install -y -q --no-install-recommends dkms"
      configure_dkms_certs

      local cache_restored=0
      if restore_dkms_cache "${NVIDIA_DRIVER_VERSION}"; then
        cache_restored=1
      fi

      local cuda_toolkit_package="cuda-toolkit"
      if [[ "${CUDA_VERSION}" != "latest" ]]; then
        cuda_toolkit_package="cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"
      fi

      # Install latest CUDA toolkit and compatible NVIDIA driver
      execute_with_retries "apt-get install -y -q --no-install-recommends ${cuda_toolkit_package}"
      execute_with_retries "apt-get install -y -q --no-install-recommends nvidia-driver-${NVIDIA_DRIVER_VERSION_PREFIX}-open"

      if [[ "${cache_restored}" -eq 0 ]]; then
        save_dkms_cache "${NVIDIA_DRIVER_VERSION}" || true
      fi

      clear_dkms_key
      modprobe nvidia

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

      local cache_restored=0
      if restore_dkms_cache "${NVIDIA_DRIVER_VERSION}"; then
        cache_restored=1
      fi

      # Special handling for driver 570 which may not be in local CUDA repo
      if [[ "${NVIDIA_DRIVER_VERSION_PREFIX}" == "570" ]]; then
        # First install CUDA toolkit from local repo (this will install driver 560)
        execute_with_retries "apt-get install -y -q --no-install-recommends cuda-toolkit-${CUDA_VERSION_MAJOR//./-}"

        # Then upgrade to driver 570 from graphics-drivers PPA
        execute_with_retries "apt-get install -y -q --no-install-recommends software-properties-common"
        execute_with_retries "add-apt-repository -y ppa:graphics-drivers/ppa"
        execute_with_retries "apt-get update"
        execute_with_retries "apt-get install -y -q --no-install-recommends nvidia-driver-${NVIDIA_DRIVER_VERSION_PREFIX}-open"
      else
        # Standard installation from local CUDA repo
        for pkg in "nvidia-driver-${NVIDIA_DRIVER_VERSION_PREFIX}-open" \
                   "cuda-drivers-${NVIDIA_DRIVER_VERSION_PREFIX}" \
                   "cuda-toolkit-${CUDA_VERSION_MAJOR//./-}" ; do
          execute_with_retries "apt-get install -y -q --no-install-recommends ${pkg}"
        done
      fi

      if [[ "${cache_restored}" -eq 0 ]]; then
        save_dkms_cache "${NVIDIA_DRIVER_VERSION}" || true
      fi

      clear_dkms_key

      modprobe nvidia
    fi


    # enable a systemd service that updates kernel headers after reboot
    setup_systemd_update_headers
    # prevent auto upgrading nvidia packages
    hold_nvidia_packages

  elif is_rocky ; then

    # Install kernel development packages
    execute_with_retries "dnf install -y kernel-devel-$(uname -r) kernel-headers-$(uname -r)"

    # Download the CUDA installer run file
    curl -fsSL --retry-connrefused --retry 3 --retry-max-time 30 -o driver.run \
        "https://developer.download.nvidia.com/compute/cuda/${CUDA_VERSION}/local_installers/cuda_${CUDA_VERSION}_${NVIDIA_DRIVER_VERSION}_linux.run"

    # Run the installer in silent mode
    execute_with_retries "bash driver.run --silent --driver --toolkit --no-opengl-libs"

    # Remove the installer file after installation to clean up
    rm driver.run

    # Load the NVIDIA kernel module
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
  test -d compute-gpu-monitoring || \
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

}

function configure_gpu_exclusive_mode() {
  # check if running spark 3 or 4, if not, enable GPU exclusive mode
  local spark_version
  spark_version=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
  if [[ ${spark_version} != 3.* ]] && [[ ${spark_version} != 4.* ]]; then
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

  # This configuration should be run on all nodes
  # regardless if they have attached GPUs
  configure_yarn

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

    if is_debian || is_ubuntu ; then
      execute_with_retries "apt-get install -y -q 'linux-headers-$(uname -r)'"
    elif is_rocky ; then
      echo "kernel devel and headers not required on rocky.  installing from binary"
    fi

    # if mig is enabled drivers would have already been installed
    if [[ $IS_MIG_ENABLED -eq 0 ]] && ! command -v nvidia-smi &>/dev/null; then
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
  if is_debian ; then
    if ! is_debian10 && ! is_debian11 && ! is_debian12 ; then
      echo "Error: The Debian version ($(os_version)) is not supported. Please use a compatible Debian version."
      exit 1
    fi
  elif is_ubuntu ; then
    if ! is_ubuntu18 && ! is_ubuntu20 && ! is_ubuntu22 && ! is_ubuntu24 ; then
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
  elif [[ "${SECURE_BOOT}" == "enabled" ]] && [[ -z "${PSN}" ]] && ! command -v nvidia-smi >/dev/null 2>&1; then
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

  # Also handle expired release files for EOL distros (like Debian 11 security repo)
  echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99ignore-valid-until

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

  # Remove security lines for EOL distros (Debian 10 and 11)
  if is_debian10 || is_debian11; then
    matched_files=( $(test -d /etc/apt && grep -rsil 'debian-security' /etc/apt/sources.list*||:) )
    if [[ -n "${matched_files[0]:-}" ]]; then
      for filename in "${matched_files[@]}"; do
        perl -pi -e "s{^.*debian-security.*$}{}g" "${filename}"
      done
    fi
  fi
}


function audit_environment() {
  echo "=== Phase 1: Audit Environment ==="

  AUDIT_GPU_HARDWARE="ABSENT"
  if lspci 2>/dev/null | grep -q NVIDIA || [[ $(get_sysfs_gpu_count 2>/dev/null || echo 0) -gt 0 ]]; then
    AUDIT_GPU_HARDWARE="PRESENT"
  fi
  export AUDIT_GPU_HARDWARE

  AUDIT_NVIDIA_DRIVER="NOT_INSTALLED"
  AUDIT_NVIDIA_DRIVER_VER="none"
  if command -v nvidia-smi > /dev/null; then
    AUDIT_NVIDIA_DRIVER="INSTALLED"
    AUDIT_NVIDIA_DRIVER_VER=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -n1 || echo "unknown")
  elif [[ -f /proc/driver/nvidia/version ]]; then
     AUDIT_NVIDIA_DRIVER="INSTALLED"
     AUDIT_NVIDIA_DRIVER_VER=$(awk '/Module Version/ {print $3}' /proc/driver/nvidia/version || echo "unknown")
  fi
  export AUDIT_NVIDIA_DRIVER AUDIT_NVIDIA_DRIVER_VER

  AUDIT_CUDA_TOOLKIT="NOT_INSTALLED"
  AUDIT_CUDA_VER="none"
  if command -v nvcc > /dev/null; then
    AUDIT_CUDA_TOOLKIT="INSTALLED"
    AUDIT_CUDA_VER=$(nvcc --version | sed -n 's/.*release \([0-9.]\+\).*/\1/p' || echo "unknown")
  elif [[ -d /usr/local/cuda ]]; then
    AUDIT_CUDA_TOOLKIT="INSTALLED"
    if [[ -f /usr/local/cuda/version.txt ]]; then
       AUDIT_CUDA_VER=$(cat /usr/local/cuda/version.txt | awk '{print $3}' || echo "unknown")
    fi
  fi
  export AUDIT_CUDA_TOOLKIT AUDIT_CUDA_VER

  AUDIT_SPARK_RAPIDS_JAR="NOT_INSTALLED"
  AUDIT_SPARK_RAPIDS_JAR_FILE=""
  AUDIT_SPARK_RAPIDS_VER="none"
  if compgen -G "/usr/lib/spark/jars/rapids-4-spark_*.jar" > /dev/null; then
    AUDIT_SPARK_RAPIDS_JAR="INSTALLED"
    AUDIT_SPARK_RAPIDS_JAR_FILE=$(compgen -G "/usr/lib/spark/jars/rapids-4-spark_*.jar" | head -n1)
    basename=$(basename "${AUDIT_SPARK_RAPIDS_JAR_FILE}")
    version_part=${basename#*_}
    version_part=${version_part%.jar}
    AUDIT_SPARK_RAPIDS_VER=${version_part#*-}
    AUDIT_SPARK_RAPIDS_VER=${AUDIT_SPARK_RAPIDS_VER%-cuda12}
  fi
  export AUDIT_SPARK_RAPIDS_JAR AUDIT_SPARK_RAPIDS_JAR_FILE AUDIT_SPARK_RAPIDS_VER

  AUDIT_XGBOOST_JAR="NOT_INSTALLED"
  AUDIT_XGBOOST_JAR_FILE=""
  AUDIT_XGBOOST_VER="none"
  if compgen -G "/usr/lib/spark/jars/xgboost4j-spark-gpu_*.jar" > /dev/null; then
    AUDIT_XGBOOST_JAR="INSTALLED"
    AUDIT_XGBOOST_JAR_FILE=$(compgen -G "/usr/lib/spark/jars/xgboost4j-spark-gpu_*.jar" | head -n1)
    basename=$(basename "${AUDIT_XGBOOST_JAR_FILE}")
    version_part=${basename#*_}
    version_part=${version_part%.jar}
    AUDIT_XGBOOST_VER=${version_part#*-}
  fi
  export AUDIT_XGBOOST_JAR AUDIT_XGBOOST_JAR_FILE AUDIT_XGBOOST_VER

  AUDIT_YARN_GPU_CONFIG="NOT_CONFIGURED"
  if [[ -f "${HADOOP_CONF_DIR}/yarn-site.xml" ]] && grep -q "yarn.io/gpu" "${HADOOP_CONF_DIR}/yarn-site.xml" 2>/dev/null; then
    AUDIT_YARN_GPU_CONFIG="CONFIGURED"
  fi
  export AUDIT_YARN_GPU_CONFIG

  AUDIT_GPU_AGENT="NOT_INSTALLED"
  if systemctl is-active --quiet google_gpu_monitoring_agent_venv.service 2>/dev/null; then
    AUDIT_GPU_AGENT="ACTIVE"
  fi
  export AUDIT_GPU_AGENT

  echo "- GPU Hardware: ${AUDIT_GPU_HARDWARE}"
  echo "- NVIDIA Driver: ${AUDIT_NVIDIA_DRIVER} (${AUDIT_NVIDIA_DRIVER_VER})"
  echo "- CUDA Toolkit: ${AUDIT_CUDA_TOOLKIT} (${AUDIT_CUDA_VER})"
  echo "- Spark RAPIDS JAR: ${AUDIT_SPARK_RAPIDS_JAR} (Ver: ${AUDIT_SPARK_RAPIDS_VER}, File: ${AUDIT_SPARK_RAPIDS_JAR_FILE:-none})"
  echo "- XGBoost GPU JAR: ${AUDIT_XGBOOST_JAR} (Ver: ${AUDIT_XGBOOST_VER})"
  echo "- YARN GPU Config: ${AUDIT_YARN_GPU_CONFIG}"
  echo "- GPU Monitoring Agent: ${AUDIT_GPU_AGENT}"
  echo "-----------------------------------"
}

PLAN_ACTIONS=()

function plan_installation() {
  echo "=== Phase 2: Generate Plan ==="
  PLAN_ACTIONS=()

  # Driver and CUDA toolkit installation needed if GPU hardware present but driver or CUDA toolkit missing
  if [[ "${AUDIT_GPU_HARDWARE}" == "PRESENT" ]] && { [[ "${AUDIT_NVIDIA_DRIVER}" != "INSTALLED" ]] || [[ "${AUDIT_CUDA_TOOLKIT}" != "INSTALLED" ]]; }; then
    PLAN_ACTIONS+=("INSTALL_NVIDIA_DRIVER")
  else
    echo "- Skip NVIDIA Driver & CUDA Toolkit installation (Driver: ${AUDIT_NVIDIA_DRIVER}, CUDA: ${AUDIT_CUDA_TOOLKIT})"
  fi

  # Spark RAPIDS and XGBoost JARs needed if missing or wrong version
  local need_rapids_update=0
  local need_xgboost_update=0

  if [[ "${AUDIT_SPARK_RAPIDS_JAR}" != "INSTALLED" ]] || [[ "${AUDIT_SPARK_RAPIDS_VER}" != "${SPARK_RAPIDS_VERSION}" ]]; then
    need_rapids_update=1
    echo "- RAPIDS JAR update needed: Installed=${AUDIT_SPARK_RAPIDS_VER}, Target=${SPARK_RAPIDS_VERSION}"
  fi

  if [[ "${AUDIT_XGBOOST_JAR}" != "INSTALLED" ]] || [[ "${AUDIT_XGBOOST_VER}" != "${XGBOOST_VERSION}" ]]; then
    need_xgboost_update=1
    echo "- XGBoost JAR update needed: Installed=${AUDIT_XGBOOST_VER}, Target=${XGBOOST_VERSION}"
  fi

  if [[ ${need_rapids_update} -eq 1 ]] || [[ ${need_xgboost_update} -eq 1 ]]; then
    if [[ "${AUDIT_SPARK_RAPIDS_JAR}" == "INSTALLED" ]] || [[ "${AUDIT_XGBOOST_JAR}" == "INSTALLED" ]]; then
      PLAN_ACTIONS+=("REPLACE_SPARK_RAPIDS_AND_XGBOOST_JARS")
    else
      PLAN_ACTIONS+=("INSTALL_SPARK_RAPIDS_AND_XGBOOST_JARS")
    fi
  else
    echo "- Skip RAPIDS & XGBoost JAR installation (Up to date)"
  fi

  # YARN GPU configuration needed if missing
  if [[ "${AUDIT_YARN_GPU_CONFIG}" != "CONFIGURED" ]]; then
    PLAN_ACTIONS+=("CONFIGURE_YARN_GPU")
  fi



  echo "- Planned Actions:"
  for action in "${PLAN_ACTIONS[@]}"; do
    echo "  * ${action}"
  done
  echo "-----------------------------------"
}

function execute_plan() {
  echo "=== Phase 3: Execute Plan ==="

  if is_debian || is_ubuntu ; then
    execute_with_retries "apt-get --allow-releaseinfo-change update"
  fi

  for action in "${PLAN_ACTIONS[@]}"; do
    case "${action}" in
      INSTALL_NVIDIA_DRIVER)
        echo "Executing: INSTALL_NVIDIA_DRIVER"
        install_nvidia_gpu_driver
        ;;
      INSTALL_SPARK_RAPIDS_AND_XGBOOST_JARS)
        echo "Executing: INSTALL_SPARK_RAPIDS_AND_XGBOOST_JARS"
        install_spark_rapids
        ;;
      REPLACE_SPARK_RAPIDS_AND_XGBOOST_JARS)
        echo "Executing: REPLACE_SPARK_RAPIDS_AND_XGBOOST_JARS"
        rm -f /usr/lib/spark/jars/rapids-4-spark_*.jar
        rm -f /usr/lib/spark/jars/xgboost4j-*.jar
        install_spark_rapids
        ;;
      CONFIGURE_YARN_GPU)
        echo "Executing: CONFIGURE_YARN_GPU"
        setup_gpu_yarn
        configure_spark
        ;;
    esac
  done
}

function main() {
  audit_environment
  plan_installation
  if is_debian && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    remove_old_backports
  fi
  check_os_and_secure_boot

  execute_plan

  # Always ensure services are restarted/running if we did configure YARN
  for svc in resourcemanager nodemanager; do
    if [[ $(systemctl show hadoop-yarn-${svc}.service -p SubState --value) == 'running' ]]; then
      systemctl restart hadoop-yarn-${svc}.service
    fi
  done

  if is_debian || is_ubuntu ; then
    apt-get clean
  fi
}

main

