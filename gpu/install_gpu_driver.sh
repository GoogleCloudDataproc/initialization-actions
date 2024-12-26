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
# This initialization action is generated from
# initialization-actions/templates/gpu/install_gpu_driver.sh.in
#
# Modifications made directly to the generated file will be lost when
# the template is re-evaluated

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

function define_os_comparison_functions() {

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
}

define_os_comparison_functions

function is_debuntu()  ( set +x ;  is_debian || is_ubuntu ; )

function os_vercat()   ( set +x
  if   is_ubuntu ; then os_version | sed -e 's/[^0-9]//g'
  elif is_rocky  ; then os_version | sed -e 's/[^0-9].*$//g'
                   else os_version ; fi ; )

function repair_old_backports {
  if ! is_debuntu ; then return ; fi
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

# replicates /usr/share/google/get_metadata_value
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

function execute_with_retries() (
  set +x
  local -r cmd="$*"

  if [[ "$cmd" =~ "^apt-get install" ]] ; then
    apt-get -y clean
    apt-get -o DPkg::Lock::Timeout=60 -y autoremove
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

function cache_fetched_package() {
  local src_url="$1"
  local gcs_fn="$2"
  local local_fn="$3"

  if gsutil ls "${gcs_fn}" 2>&1 | grep -q "${gcs_fn}" ; then
    time gcloud storage cp "${gcs_fn}" "${local_fn}"
  else
    time ( curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 "${src_url}" -o "${local_fn}" && \
           gcloud storage cp "${local_fn}" "${gcs_fn}" ; )
  fi
}

function add_contrib_component() {
  if ! is_debuntu ; then return ; fi
  if ge_debian12 ; then
      # Include in sources file components on which nvidia-kernel-open-dkms depends
      local -r debian_sources="/etc/apt/sources.list.d/debian.sources"
      local components="main contrib"

      sed -i -e "s/Components: .*$/Components: ${components}/" "${debian_sources}"
  elif is_debian ; then
      sed -i -e 's/ main$/ main contrib/' /etc/apt/sources.list
  fi
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

function set_proxy(){
  METADATA_HTTP_PROXY="$(get_metadata_attribute http-proxy '')"

  if [[ -z "${METADATA_HTTP_PROXY}" ]] ; then return ; fi

  export METADATA_HTTP_PROXY
  export http_proxy="${METADATA_HTTP_PROXY}"
  export https_proxy="${METADATA_HTTP_PROXY}"
  export HTTP_PROXY="${METADATA_HTTP_PROXY}"
  export HTTPS_PROXY="${METADATA_HTTP_PROXY}"
  no_proxy="localhost,127.0.0.0/8,::1,metadata.google.internal,169.254.169.254"
  local no_proxy_svc
  for no_proxy_svc in compute  secretmanager dns    servicedirectory     logging  \
                      bigquery composer      pubsub bigquerydatatransfer dataflow \
                      storage  datafusion    ; do
    no_proxy="${no_proxy},${no_proxy_svc}.googleapis.com"
  done

  export NO_PROXY="${no_proxy}"
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

function check_os() {
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

  SPARK_VERSION="$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)"
  readonly SPARK_VERSION
  if version_lt "${SPARK_VERSION}" "3.1" || \
     version_ge "${SPARK_VERSION}" "4.0" ; then
    echo "Error: Your Spark version is not supported. Please upgrade Spark to one of the supported versions."
    exit 1
  fi

  # Detect dataproc image version
  if (! test -v DATAPROC_IMAGE_VERSION) ; then
    if test -v DATAPROC_VERSION ; then
      DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
    else
      if   version_lt "${SPARK_VERSION}" "3.2" ; then DATAPROC_IMAGE_VERSION="2.0"
      elif version_lt "${SPARK_VERSION}" "3.4" ; then DATAPROC_IMAGE_VERSION="2.1"
      elif version_lt "${SPARK_VERSION}" "3.6" ; then DATAPROC_IMAGE_VERSION="2.2"
      else echo "Unknown dataproc image version" ; exit 1 ; fi
    fi
  fi
}

#
# Generate repo file under /etc/apt/sources.list.d/
#
function apt_add_repo() {
  local -r repo_name="$1"
  local -r repo_data="$3" # "http(s)://host/path/uri argument0 .. argumentN"
  local -r include_src="${4:-yes}"
  local -r kr_path="${5:-/usr/share/keyrings/${repo_name}.gpg}"
  local -r repo_path="${6:-/etc/apt/sources.list.d/${repo_name}.list}"

  echo "deb [signed-by=${kr_path}] ${repo_data}" > "${repo_path}"
  if [[ "${include_src}" == "yes" ]] ; then
    echo "deb-src [signed-by=${kr_path}] ${repo_data}" >> "${repo_path}"
  fi

  apt-get update -qq
}

#
# Generate repo file under /etc/yum.repos.d/
#
function dnf_add_repo() {
  local -r repo_name="$1"
  local -r repo_url="$3" # "http(s)://host/path/filename.repo"
  local -r kr_path="${5:-/etc/pki/rpm-gpg/${repo_name}.gpg}"
  local -r repo_path="${6:-/etc/yum.repos.d/${repo_name}.repo}"

  curl -s -L "${repo_url}" \
    | dd of="${repo_path}" status=progress
#    | perl -p -e "s{^gpgkey=.*$}{gpgkey=file://${kr_path}}" \
}

#
# Keyrings default to
# /usr/share/keyrings/${repo_name}.gpg (debian/ubuntu) or
# /etc/pki/rpm-gpg/${repo_name}.gpg    (rocky/RHEL)
#
function os_add_repo() {
  local -r repo_name="$1"
  local -r signing_key_url="$2"
  local -r repo_data="$3" # "http(s)://host/path/uri argument0 .. argumentN"
  local kr_path
  if is_debuntu ; then kr_path="${5:-/usr/share/keyrings/${repo_name}.gpg}"
                  else kr_path="${5:-/etc/pki/rpm-gpg/${repo_name}.gpg}" ; fi

  mkdir -p "$(dirname "${kr_path}")"

  curl -fsS --retry-connrefused --retry 10 --retry-max-time 30 "${signing_key_url}" \
    | gpg --import --no-default-keyring --keyring "${kr_path}"

  if is_debuntu ; then apt_add_repo "${repo_name}" "${signing_key_url}" "${repo_data}" "${4:-yes}" "${kr_path}" "${6:-}"
                  else dnf_add_repo "${repo_name}" "${signing_key_url}" "${repo_data}" "${4:-yes}" "${kr_path}" "${6:-}" ; fi
}


readonly _shortname="$(os_id)$(os_version|perl -pe 's/(\d+).*/$1/')"

# Dataproc configurations
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly SPARK_CONF_DIR='/etc/spark/conf'


function configure_dkms_certs() {
  if test -v PSN && [[ -z "${PSN}" ]]; then
      echo "No signing secret provided.  skipping";
      return 0
  fi

  mkdir -p "${CA_TMPDIR}"

  # If the private key exists, verify it
  if [[ -f "${CA_TMPDIR}/db.rsa" ]]; then
    echo "Private key material exists"

    local expected_modulus_md5sum
    expected_modulus_md5sum=$(get_metadata_attribute modulus_md5sum)
    if [[ -n "${expected_modulus_md5sum}" ]]; then
      modulus_md5sum="${expected_modulus_md5sum}"

      # Verify that cert md5sum matches expected md5sum
      if [[ "${modulus_md5sum}" != "$(openssl rsa -noout -modulus -in "${CA_TMPDIR}/db.rsa" | openssl md5 | awk '{print $2}')" ]]; then
        echo "unmatched rsa key"
      fi

      # Verify that key md5sum matches expected md5sum
      if [[ "${modulus_md5sum}" != "$(openssl x509 -noout -modulus -in ${mok_der} | openssl md5 | awk '{print $2}')" ]]; then
        echo "unmatched x509 cert"
      fi
    else
      modulus_md5sum="$(openssl rsa -noout -modulus -in "${CA_TMPDIR}/db.rsa" | openssl md5 | awk '{print $2}')"
    fi
    ln -sf "${CA_TMPDIR}/db.rsa" "${mok_key}"

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

  local mok_directory="$(dirname "${mok_key}")"
  mkdir -p "${mok_directory}"

  # symlink private key and copy public cert from volatile storage to DKMS directory
  ln -sf "${CA_TMPDIR}/db.rsa" "${mok_key}"
  cp  -f "${CA_TMPDIR}/db.der" "${mok_der}"

  modulus_md5sum="$(openssl rsa -noout -modulus -in "${mok_key}" | openssl md5 | awk '{print $2}')"
}

function clear_dkms_key {
  if [[ -z "${PSN}" ]]; then
      echo "No signing secret provided.  skipping" >&2
      return 0
  fi
  rm -rf "${CA_TMPDIR}" "${mok_key}"
}

function check_secure_boot() {
  local SECURE_BOOT="disabled"
  SECURE_BOOT=$(mokutil --sb-state|awk '{print $2}')

  PSN="$(get_metadata_attribute private_secret_name)"
  readonly PSN

  if [[ "${SECURE_BOOT}" == "enabled" ]] && le_debian11 ; then
    echo "Error: Secure Boot is not supported on Debian before image 2.2. Please disable Secure Boot while creating the cluster."
    exit 1
  elif [[ "${SECURE_BOOT}" == "enabled" ]] && [[ -z "${PSN}" ]]; then
    echo "Secure boot is enabled, but no signing material provided."
    echo "Please either disable secure boot or provide signing material as per"
    echo "https://github.com/GoogleCloudDataproc/custom-images/tree/master/examples/secure-boot"
    return 1
  fi

  CA_TMPDIR="$(mktemp -u -d -p /run/tmp -t ca_dir-XXXX)"
  readonly CA_TMPDIR

  if is_ubuntu ; then mok_key=/var/lib/shim-signed/mok/MOK.priv
                      mok_der=/var/lib/shim-signed/mok/MOK.der
                 else mok_key=/var/lib/dkms/mok.key
                      mok_der=/var/lib/dkms/mok.pub ; fi

  configure_dkms_certs
}


function set_support_matrix() {
  # CUDA version and Driver version
  # https://docs.nvidia.com/deploy/cuda-compatibility/
  # https://docs.nvidia.com/deeplearning/frameworks/support-matrix/index.html
  # https://developer.nvidia.com/cuda-downloads

  # Minimum supported version for open kernel driver is 515.43.04
  # https://github.com/NVIDIA/open-gpu-kernel-modules/tags
  # Rocky8: 12.0: 525.147.05
  local latest
  latest="$(curl -s https://download.nvidia.com/XFree86/Linux-x86_64/latest.txt | awk '{print $1}')"
  readonly -A DRIVER_FOR_CUDA=(
          ["11.7"]="515.65.01"   ["11.8"]="525.147.05"
          ["12.0"]="525.147.05"  ["12.1"]="530.30.02" ["12.4"]="550.135"    ["12.5"]="555.42.02"  ["12.6"]="560.35.03"
  )
  readonly -A DRIVER_SUBVER=(
          ["515"]="515.48.07"   ["520"]="525.147.05" ["525"]="525.147.05"  ["530"]="530.41.03"   ["535"]="535.216.01"
          ["545"]="545.29.06"   ["550"]="550.135"    ["555"]="555.58.02"   ["560"]="560.35.03"   ["565"]="565.57.01"
  )
  # https://developer.nvidia.com/cudnn-downloads
  if is_debuntu ; then
  readonly -A CUDNN_FOR_CUDA=(
          ["11.7"]="9.5.1.17"   ["11.8"]="9.5.1.17"
          ["12.0"]="9.5.1.17"   ["12.1"]="9.5.1.17"  ["12.4"]="9.5.1.17"   ["12.5"]="9.5.1.17"   ["12.6"]="9.5.1.17"
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
          ["11.7"]="8.9.7.29"   ["11.8"]="9.5.1.17"
          ["12.0"]="8.8.1.3"    ["12.1"]="8.9.3.28"  ["12.4"]="9.1.1.17"   ["12.5"]="9.2.1.18"   ["12.6"]="9.5.1.17"
  )
  fi
  # https://developer.nvidia.com/nccl/nccl-download
  # 12.2: 2.19.3, 12.5: 2.21.5
  readonly -A NCCL_FOR_CUDA=(
          ["11.7"]="2.21.5"     ["11.8"]="2.21.5"
          ["12.0"]="2.16.5"     ["12.1"]="2.18.3"    ["12.4"]="2.23.4"     ["12.5"]="2.21.5"     ["12.6"]="2.23.4"
  )
  readonly -A CUDA_SUBVER=(
          ["11.7"]="11.7.1"     ["11.8"]="11.8.0"
          ["12.0"]="12.0.1"     ["12.1"]="12.1.1"    ["12.2"]="12.2.2"     ["12.3"]="12.3.2"     ["12.4"]="12.4.1"     ["12.5"]="12.5.1"     ["12.6"]="12.6.2"
  )
}

set_support_matrix

function set_cuda_version() {
  local cuda_url
  cuda_url=$(get_metadata_attribute 'cuda-url' '')
  if [[ -n "${cuda_url}" ]] ; then
    # if cuda-url metadata variable has been passed, extract default version from url
    local CUDA_URL_VERSION
    CUDA_URL_VERSION="$(echo "${cuda_url}" | perl -pe 's{^.*/cuda_(\d+\.\d+\.\d+)_\d+\.\d+\.\d+_linux.run$}{$1}')"
    if [[ "${CUDA_URL_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ; then
      DEFAULT_CUDA_VERSION="${CUDA_URL_VERSION%.*}"
      CUDA_FULL_VERSION="${CUDA_URL_VERSION}"
    fi
  fi

  if ( ! test -v DEFAULT_CUDA_VERSION ) ; then
    DEFAULT_CUDA_VERSION='12.4'
  fi
  readonly DEFAULT_CUDA_VERSION

  CUDA_VERSION=$(get_metadata_attribute 'cuda-version' "${DEFAULT_CUDA_VERSION}")
  if test -n "$(echo "${CUDA_VERSION}" | perl -ne 'print if /\d+\.\d+\.\d+/')" ; then
    CUDA_FULL_VERSION="${CUDA_VERSION}"
    CUDA_VERSION="${CUDA_VERSION%.*}"
  fi
  readonly CUDA_VERSION
  if ( ! test -v CUDA_FULL_VERSION ) ; then
    CUDA_FULL_VERSION=${CUDA_SUBVER["${CUDA_VERSION}"]}
  fi
  readonly CUDA_FULL_VERSION

}

set_cuda_version

function is_cuda12() ( set +x ; [[ "${CUDA_VERSION%%.*}" == "12" ]] ; )
function le_cuda12() ( set +x ; version_le "${CUDA_VERSION%%.*}" "12" ; )
function ge_cuda12() ( set +x ; version_ge "${CUDA_VERSION%%.*}" "12" ; )

function is_cuda11() ( set +x ; [[ "${CUDA_VERSION%%.*}" == "11" ]] ; )
function le_cuda11() ( set +x ; version_le "${CUDA_VERSION%%.*}" "11" ; )
function ge_cuda11() ( set +x ; version_ge "${CUDA_VERSION%%.*}" "11" ; )

function set_driver_version() {
  local gpu_driver_url
  gpu_driver_url=$(get_metadata_attribute 'gpu-driver-url' '')

  local cuda_url
  cuda_url=$(get_metadata_attribute 'cuda-url' '')

  local DEFAULT_DRIVER
  # Take default from gpu-driver-url metadata value
  if [[ -n "${gpu_driver_url}" ]] ; then
    DRIVER_URL_DRIVER_VERSION="$(echo "${gpu_driver_url}" | perl -pe 's{^.*/NVIDIA-Linux-x86_64-(\d+\.\d+\.\d+).run$}{$1}')"
    if [[ "${DRIVER_URL_DRIVER_VERSION}" =~ ^[0-9]+.*[0-9]$ ]] ; then DEFAULT_DRIVER="${DRIVER_URL_DRIVER_VERSION}" ; fi
  # Take default from cuda-url metadata value as a backup
  elif [[ -n "${cuda_url}" ]] ; then
    local CUDA_URL_DRIVER_VERSION="$(echo "${cuda_url}" | perl -pe 's{^.*/cuda_\d+\.\d+\.\d+_(\d+\.\d+\.\d+)_linux.run$}{$1}')"
    if [[ "${CUDA_URL_DRIVER_VERSION}" =~ ^[0-9]+.*[0-9]$ ]] ; then
      major_driver_version="${CUDA_URL_DRIVER_VERSION%%.*}"
      driver_max_maj_version=${DRIVER_SUBVER["${major_driver_version}"]}
      if curl -s --head "https://download.nvidia.com/XFree86/Linux-x86_64/${CUDA_URL_DRIVER_VERSION}/NVIDIA-Linux-x86_64-${CUDA_URL_DRIVER_VERSION}.run" | grep -E -q '^HTTP.*200\s*$' ; then
        # use the version indicated by the cuda url as the default if it exists
	DEFAULT_DRIVER="${CUDA_URL_DRIVER_VERSION}"
      elif curl -s --head "https://download.nvidia.com/XFree86/Linux-x86_64/${driver_max_maj_version}/NVIDIA-Linux-x86_64-${driver_max_maj_version}.run" | grep -E -q '^HTTP.*200\s*$' ; then
        # use the maximum sub-version available for the major version indicated in cuda url as the default
	DEFAULT_DRIVER="${driver_max_maj_version}"
      fi
    fi
  fi

  if ( ! test -v DEFAULT_DRIVER ) ; then
    # If a default driver version has not been extracted, use the default for this version of CUDA
    DEFAULT_DRIVER=${DRIVER_FOR_CUDA["${CUDA_VERSION}"]}
  fi

  DRIVER_VERSION=$(get_metadata_attribute 'gpu-driver-version' "${DEFAULT_DRIVER}")

  readonly DRIVER_VERSION
  readonly DRIVER="${DRIVER_VERSION%%.*}"

  export DRIVER_VERSION DRIVER

  gpu_driver_url="https://download.nvidia.com/XFree86/Linux-x86_64/${DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DRIVER_VERSION}.run"
  if ! curl -s --head "${gpu_driver_url}" | grep -E -q '^HTTP.*200\s*$' ; then
    echo "No NVIDIA driver exists for DRIVER_VERSION=${DRIVER_VERSION}"
    exit 1
  fi
}

set_driver_version

function set_cudnn_version() {
  readonly DEFAULT_CUDNN8_VERSION="8.0.5.39"
  readonly DEFAULT_CUDNN9_VERSION="9.1.0.70"

  # Parameters for NVIDIA-provided cuDNN library
  readonly DEFAULT_CUDNN_VERSION=${CUDNN_FOR_CUDA["${CUDA_VERSION}"]}
  CUDNN_VERSION=$(get_metadata_attribute 'cudnn-version' "${DEFAULT_CUDNN_VERSION}")
  # The minimum cuDNN version supported by rocky is ${DEFAULT_CUDNN8_VERSION}
  if is_rocky  && (version_le "${CUDNN_VERSION}" "${DEFAULT_CUDNN8_VERSION}") ; then
    CUDNN_VERSION="${DEFAULT_CUDNN8_VERSION}"
  elif (ge_ubuntu20 || ge_debian12) && [[ "${CUDNN_VERSION%%.*}" == "8" ]] ; then
    # cuDNN v8 is not distribution for ubuntu20+, debian12
    CUDNN_VERSION="${DEFAULT_CUDNN9_VERSION}"
  elif (le_ubuntu18 || le_debian11) && [[ "${CUDNN_VERSION%%.*}" == "9" ]] ; then
    # cuDNN v9 is not distributed for ubuntu18, debian10, debian11 ; fall back to 8
    CUDNN_VERSION="8.8.0.121"
  fi
  readonly CUDNN_VERSION
}
set_cudnn_version

function is_cudnn8() ( set +x ; [[ "${CUDNN_VERSION%%.*}" == "8" ]] ; )
function is_cudnn9() ( set +x ; [[ "${CUDNN_VERSION%%.*}" == "9" ]] ; )

readonly DEFAULT_NCCL_VERSION=${NCCL_FOR_CUDA["${CUDA_VERSION}"]}
readonly NCCL_VERSION=$(get_metadata_attribute 'nccl-version' ${DEFAULT_NCCL_VERSION})

# Parameters for NVIDIA-provided Debian GPU driver
readonly DEFAULT_USERSPACE_URL="https://download.nvidia.com/XFree86/Linux-x86_64/${DRIVER_VERSION}/NVIDIA-Linux-x86_64-${DRIVER_VERSION}.run"

readonly USERSPACE_URL=$(get_metadata_attribute 'gpu-driver-url' "${DEFAULT_USERSPACE_URL}")

USERSPACE_FILENAME="$(echo ${USERSPACE_URL} | perl -pe 's{^.+/}{}')"
readonly USERSPACE_FILENAME

# Short name for urls
if is_ubuntu22  ; then
    # at the time of writing 20241125 there is no ubuntu2204 in the index of repos at
    # https://developer.download.nvidia.com/compute/machine-learning/repos/
    # use packages from previous release until such time as nvidia
    # release ubuntu2204 builds

    shortname="$(os_id)$(os_vercat)"
    nccl_shortname="ubuntu2004"
elif ge_rocky9 ; then
    # use packages from previous release until such time as nvidia
    # release rhel9 builds

    shortname="rhel9"
    nccl_shortname="rhel8"
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
  local MAX_DRIVER_VERSION
  local MAX_CUDA_VERSION

  local MIN_OPEN_DRIVER_VER="515.48.07"
  local MIN_DRIVER_VERSION="${MIN_OPEN_DRIVER_VER}"
  local MIN_CUDA_VERSION="11.7.1" # matches MIN_OPEN_DRIVER_VER

  if is_cuda12 ; then
    if is_debian12 ; then
      MIN_DRIVER_VERSION="545.23.06"
      MIN_CUDA_VERSION="12.3.0"
    elif is_debian10 ; then
      MAX_DRIVER_VERSION="555.42.02"
      MAX_CUDA_VERSION="12.5.0"
    elif is_ubuntu18 ; then
      MAX_DRIVER_VERSION="530.30.02"
      MAX_CUDA_VERSION="12.1.1"
    fi
  elif version_ge "${CUDA_VERSION}" "${MIN_CUDA_VERSION}" ; then
    if le_debian10 ; then
      # cuda 11 is not supported for <= debian10
      MAX_CUDA_VERSION="0"
      MAX_DRIVER_VERSION="0"
    fi
  else
    echo "Minimum CUDA version supported is ${MIN_CUDA_VERSION}.  Specified: ${CUDA_VERSION}"
  fi

  if version_lt "${CUDA_VERSION}" "${MIN_CUDA_VERSION}" ; then
    echo "Minimum CUDA version for ${shortname} is ${MIN_CUDA_VERSION}.  Specified: ${CUDA_VERSION}"
  elif ( test -v MAX_CUDA_VERSION && version_gt "${CUDA_VERSION}" "${MAX_CUDA_VERSION}" ) ; then
    echo "Maximum CUDA version for ${shortname} is ${MAX_CUDA_VERSION}.  Specified: ${CUDA_VERSION}"
  fi
  if version_lt "${DRIVER_VERSION}" "${MIN_DRIVER_VERSION}" ; then
    echo "Minimum kernel driver version for ${shortname} is ${MIN_DRIVER_VERSION}.  Specified: ${DRIVER_VERSION}"
  elif ( test -v MAX_DRIVER_VERSION && version_gt "${DRIVER_VERSION}" "${MAX_DRIVER_VERSION}" ) ; then
    echo "Maximum kernel driver version for ${shortname} is ${MAX_DRIVER_VERSION}.  Specified: ${DRIVER_VERSION}"
  fi

  # driver version named in cuda runfile filename
  # (these may not be actual driver versions - see https://download.nvidia.com/XFree86/Linux-x86_64/)
  readonly -A drv_for_cuda=(
          ["11.7.0"]="515.43.04" ["11.7.1"]="515.65.01"
          ["11.8.0"]="520.61.05"
          ["12.0.0"]="525.60.13" ["12.0.1"]="525.85.12"
          ["12.1.0"]="530.30.02" ["12.1.1"]="530.30.02"
          ["12.2.0"]="535.54.03" ["12.2.1"]="535.86.10" ["12.2.2"]="535.104.05"
          ["12.3.0"]="545.23.06" ["12.3.1"]="545.23.08" ["12.3.2"]="545.23.08"
          ["12.4.0"]="550.54.14" ["12.4.1"]="550.54.15" # 550.54.15 is not a driver indexed at https://download.nvidia.com/XFree86/Linux-x86_64/
          ["12.5.0"]="555.42.02" ["12.5.1"]="555.42.06" # 555.42.02 is indexed, 555.41.06 is not
          ["12.6.0"]="560.28.03" ["12.6.1"]="560.35.03" ["12.6.2"]="560.35.03"
  )

  # Verify that the file with the indicated combination exists
  local drv_ver=${drv_for_cuda["${CUDA_FULL_VERSION}"]}
  CUDA_RUNFILE="cuda_${CUDA_FULL_VERSION}_${drv_ver}_linux.run"
  local CUDA_RELEASE_BASE_URL="${NVIDIA_BASE_DL_URL}/cuda/${CUDA_FULL_VERSION}"
  local DEFAULT_NVIDIA_CUDA_URL="${CUDA_RELEASE_BASE_URL}/local_installers/${CUDA_RUNFILE}"

  NVIDIA_CUDA_URL=$(get_metadata_attribute 'cuda-url' "${DEFAULT_NVIDIA_CUDA_URL}")
  readonly NVIDIA_CUDA_URL

  CUDA_RUNFILE="$(echo ${NVIDIA_CUDA_URL} | perl -pe 's{^.+/}{}')"
  readonly CUDA_RUNFILE

  if ! curl -s --head "${NVIDIA_CUDA_URL}" | grep -E -q '^HTTP.*200\s*$' ; then
    echo "No CUDA distribution exists for this combination of DRIVER_VERSION=${drv_ver}, CUDA_VERSION=${CUDA_FULL_VERSION}"
    exit 1
  fi

  if ( version_lt "${CUDA_FULL_VERSION}" "12.3.0" && ge_debian12 ) ; then
    echo "CUDA 12.3.0 is the minimum CUDA 12 version supported on Debian 12"
  elif ( version_gt "${CUDA_VERSION}" "12.1.1" && is_ubuntu18 ) ; then
    echo "CUDA 12.1.1 is the maximum CUDA version supported on ubuntu18.  Requested version: ${CUDA_VERSION}"
  elif ( version_lt "${CUDA_VERSION%%.*}" "12" && ge_debian12 ) ; then
    echo "CUDA 11 not supported on Debian 12. Requested version: ${CUDA_VERSION}"
  elif ( version_lt "${CUDA_VERSION}" "11.8" && is_rocky9 ) ; then
    echo "CUDA 11.8.0 is the minimum version for Rocky 9. Requested version: ${CUDA_VERSION}"
  fi
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

# Whether to install GPU monitoring agent that sends GPU metrics to Stackdriver
INSTALL_GPU_AGENT=$(get_metadata_attribute 'install-gpu-agent' 'false')
readonly INSTALL_GPU_AGENT

NVIDIA_SMI_PATH='/usr/bin'
MIG_MAJOR_CAPS=0
IS_MIG_ENABLED=0

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

function install_local_cuda_repo() {
  if test -f "${workdir}/complete/install-local-cuda-repo" ; then return ; fi

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

  touch "${workdir}/complete/install-local-cuda-repo"
}
function uninstall_local_cuda_repo(){
  apt-get purge -yq "${CUDA_LOCAL_REPO_PKG_NAME}"
  rm -f "${workdir}/complete/install-local-cuda-repo"
}

CUDNN_PKG_NAME=""
function install_local_cudnn_repo() {
  if test -f "${workdir}/complete/install-local-cudnn-repo" ; then return ; fi
  pkgname="cudnn-local-repo-${shortname}-${CUDNN_VERSION%.*}"
  CUDNN_PKG_NAME="${pkgname}"
  local_deb_fn="${pkgname}_1.0-1_amd64.deb"
  local_deb_url="${NVIDIA_BASE_DL_URL}/cudnn/${CUDNN_VERSION%.*}/local_installers/${local_deb_fn}"

  # ${NVIDIA_BASE_DL_URL}/redist/cudnn/v8.6.0/local_installers/11.8/cudnn-linux-x86_64-8.6.0.163_cuda11-archive.tar.xz
  curl -fsSL --retry-connrefused --retry 3 --retry-max-time 5 \
    "${local_deb_url}" -o "${tmpdir}/local-installer.deb"

  dpkg -i "${tmpdir}/local-installer.deb"

  rm -f "${tmpdir}/local-installer.deb"

  cp /var/cudnn-local-repo-*-${CUDNN_VERSION%.*}*/cudnn-local-*-keyring.gpg /usr/share/keyrings

  touch "${workdir}/complete/install-local-cudnn-repo"
}

function uninstall_local_cudnn_repo() {
  apt-get purge -yq "${CUDNN_PKG_NAME}"
  rm -f "${workdir}/complete/install-local-cudnn-repo"
}

CUDNN8_LOCAL_REPO_INSTALLED="0"
CUDNN8_PKG_NAME=""
function install_local_cudnn8_repo() {
  if test -f "${workdir}/complete/install-local-cudnn8-repo" ; then return ; fi

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
  local_deb_url="${NVIDIA_BASE_DL_URL}/redist/cudnn/v${CUDNN_VERSION%.*}/local_installers/${CUDNN8_CUDA_VER}/${deb_fn}"

  # cache the cudnn package
  cache_fetched_package "${local_deb_url}" \
                        "${pkg_bucket}/${CUDNN8_CUDA_VER}/${deb_fn}" \
                        "${local_deb_fn}"

  local cudnn_path="$(dpkg -c ${local_deb_fn} | perl -ne 'if(m{(/var/cudnn-local-repo-.*)/\s*$}){print $1}')"
  # If we are using a ram disk, mount another where we will unpack the cudnn local installer
  if [[ "${tmpdir}" == "/mnt/shm" ]] && ! grep -q '/var/cudnn-local-repo' /proc/mounts ; then
    mkdir -p "${cudnn_path}"
    mount -t tmpfs tmpfs "${cudnn_path}"
  fi

  dpkg -i "${local_deb_fn}"

  rm -f "${local_deb_fn}"

  cp "${cudnn_path}"/cudnn-local-*-keyring.gpg /usr/share/keyrings
  touch "${workdir}/complete/install-local-cudnn8-repo"
}

function uninstall_local_cudnn8_repo() {
  apt-get purge -yq "${CUDNN8_PKG_NAME}"
  rm -f "${workdir}/complete/install-local-cudnn8-repo"
}

function install_nvidia_nccl() {
  if test -f "${workdir}/complete/nccl" ; then return ; fi

  if is_cuda11 && is_debian12 ; then
    echo "NCCL cannot be compiled for CUDA 11 on ${_shortname}"
    return
  fi

  local -r nccl_version="${NCCL_VERSION}-1+cuda${CUDA_VERSION}"

  # https://github.com/NVIDIA/nccl/blob/master/README.md
  # https://arnon.dk/matching-sm-architectures-arch-and-gencode-for-various-nvidia-cards/
  # Fermi:     SM_20,             compute_30
  # Kepler:    SM_30,SM_35,SM_37, compute_30,compute_35,compute_37
  # Maxwell:   SM_50,SM_52,SM_53, compute_50,compute_52,compute_53
  # Pascal:    SM_60,SM_61,SM_62, compute_60,compute_61,compute_62

  # The following architectures are suppored by open kernel driver
  # Volta:     SM_70,SM_72,       compute_70,compute_72
  # Ampere:    SM_80,SM_86,SM_87, compute_80,compute_86,compute_87

  # The following architectures are supported by CUDA v11.8+
  # Ada:       SM_89,             compute_89
  # Hopper:    SM_90,SM_90a       compute_90,compute_90a
  # Blackwell: SM_100,            compute_100
                  NVCC_GENCODE="-gencode=arch=compute_70,code=sm_70 -gencode=arch=compute_72,code=sm_72"
  NVCC_GENCODE="${NVCC_GENCODE} -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_86,code=sm_86 -gencode=arch=compute_87,code=sm_87"
  if version_ge "${CUDA_VERSION}" "11.8" ; then
    NVCC_GENCODE="${NVCC_GENCODE} -gencode=arch=compute_89,code=sm_89"
  fi
  if version_ge "${CUDA_VERSION}" "12.0" ; then
    NVCC_GENCODE="${NVCC_GENCODE} -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_90a,code=compute_90a"
  fi

  mkdir -p "${workdir}"
  pushd "${workdir}"

  test -d "${workdir}/nccl" || {
    local tarball_fn="v${NCCL_VERSION}-1.tar.gz"
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "https://github.com/NVIDIA/nccl/archive/refs/tags/${tarball_fn}" \
      | tar xz
    mv "nccl-${NCCL_VERSION}-1" nccl
  }

  local build_path
  if is_debuntu ; then build_path="nccl/build/pkg/deb" ; else
                       build_path="nccl/build/pkg/rpm/x86_64" ; fi

  test -d "${workdir}/nccl/build" || {
    local build_tarball="nccl-build_${_shortname}_${nccl_version}.tar.gz"
    local local_tarball="${workdir}/${build_tarball}"
    local gcs_tarball="${pkg_bucket}/${_shortname}/${build_tarball}"

    output=$(gsutil ls "${gcs_tarball}" 2>&1 || echo '')
    if echo "${output}" | grep -q "${gcs_tarball}" ; then
      # cache hit - unpack from cache
      echo "cache hit"
    else
      # build and cache
      pushd nccl
      # https://github.com/NVIDIA/nccl?tab=readme-ov-file#install
      install_build_dependencies
      if is_debuntu ; then
        # These packages are required to build .deb packages from source
        execute_with_retries \
          apt-get install -y -qq build-essential devscripts debhelper fakeroot
        export NVCC_GENCODE
        execute_with_retries make -j$(nproc) pkg.debian.build
      elif is_rocky ; then
        # These packages are required to build .rpm packages from source
        execute_with_retries \
          dnf -y -q install rpm-build rpmdevtools
        export NVCC_GENCODE
        execute_with_retries make -j$(nproc) pkg.redhat.build
      fi
      tar czvf "/${local_tarball}" "../${build_path}"
      gcloud storage cp "${local_tarball}" "${gcs_tarball}"
      rm "${local_tarball}"
      make clean
      popd
    fi
    gcloud storage cat "${gcs_tarball}" | tar xz
  }

  if is_debuntu ; then
    dpkg -i "${build_path}/libnccl${NCCL_VERSION%%.*}_${nccl_version}_amd64.deb" "${build_path}/libnccl-dev_${nccl_version}_amd64.deb"
  elif is_rocky ; then
    rpm -ivh "${build_path}/libnccl-${nccl_version}.x86_64.rpm" "${build_path}/libnccl-devel-${nccl_version}.x86_64.rpm"
  fi

  popd
  touch "${workdir}/complete/nccl"
}

function is_src_nvidia() ( set +x ; [[ "${GPU_DRIVER_PROVIDER}" == "NVIDIA" ]] ; )
function is_src_os()     ( set +x ; [[ "${GPU_DRIVER_PROVIDER}" == "OS" ]] ; )

function install_nvidia_cudnn() {
  if test -f "${workdir}/complete/cudnn" ; then return ; fi
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
      if is_cudnn8 ; then
        install_local_cudnn8_repo

        apt-get update -qq

        execute_with_retries \
          apt-get -y install --no-install-recommends \
            "libcudnn8=${cudnn_pkg_version}" \
            "libcudnn8-dev=${cudnn_pkg_version}"

        uninstall_local_cudnn8_repo
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
  else
    echo "Unsupported OS: '${_shortname}'"
    exit 1
  fi

  ldconfig

  echo "NVIDIA cuDNN successfully installed for ${_shortname}."
  touch "${workdir}/complete/cudnn"
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

#
# Install package signing key and add corresponding repository
# https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html
function add_repo_nvidia_container_toolkit() {
  local nvctk_root="https://nvidia.github.io/libnvidia-container"
  local signing_key_url="${nvctk_root}/gpgkey"
  local repo_data

  if is_debuntu ; then repo_data="${nvctk_root}/stable/deb/\$(ARCH) /"
                  else repo_data="${nvctk_root}/stable/rpm/nvidia-container-toolkit.repo" ; fi

  os_add_repo nvidia-container-toolkit \
              "${signing_key_url}" \
              "${repo_data}" \
              "no"
}

function add_repo_cuda() {
  if is_debuntu ; then
    install_cuda_keyring_pkg # 11.7+, 12.0+
  elif is_rocky ; then
    execute_with_retries "dnf config-manager --add-repo ${NVIDIA_ROCKY_REPO_URL}"
  fi
}

function build_driver_from_github() {
  # non-GPL driver will have been built on rocky8
  if is_rocky8 ; then return 0 ; fi
  pushd "${workdir}"

  test -d "${workdir}/open-gpu-kernel-modules" || {
    local tarball_fn="${DRIVER_VERSION}.tar.gz"
    curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
      "https://github.com/NVIDIA/open-gpu-kernel-modules/archive/refs/tags/${tarball_fn}" \
      | tar xz
    mv "open-gpu-kernel-modules-${DRIVER_VERSION}" open-gpu-kernel-modules
  }

  local nvidia_ko_path="$(find /lib/modules/$(uname -r)/ -name 'nvidia.ko')"
  test -n "${nvidia_ko_path}" && test -f "${nvidia_ko_path}" || {
    local build_tarball="kmod_${_shortname}_${DRIVER_VERSION}.tar.gz"
    local local_tarball="${workdir}/${build_tarball}"
    local build_dir
    if test -v modulus_md5sum && [[ -n "${modulus_md5sum}" ]]
      then build_dir="${modulus_md5sum}"
      else build_dir="unsigned" ; fi

    local gcs_tarball="${pkg_bucket}/${_shortname}/${uname_r}/${build_dir}/${build_tarball}"

    if gsutil ls "${gcs_tarball}" 2>&1 | grep -q "${gcs_tarball}" ; then
      echo "cache hit"
    else
      # build the kernel modules
      pushd open-gpu-kernel-modules
      install_build_dependencies
      if is_cuda11 && is_ubuntu22 ; then
        echo "Kernel modules cannot be compiled for CUDA 11 on ${_shortname}"
        exit 1
      fi
      execute_with_retries make -j$(nproc) modules \
        >  kernel-open/build.log \
        2> kernel-open/build_error.log
      # Sign kernel modules
      if [[ -n "${PSN}" ]]; then
        for module in $(find open-gpu-kernel-modules/kernel-open -name '*.ko'); do
          "/lib/modules/${uname_r}/build/scripts/sign-file" sha256 \
          "${mok_key}" \
          "${mok_der}" \
          "${module}"
        done
      fi
      make modules_install \
        >>  kernel-open/build.log \
        2>> kernel-open/build_error.log
      # Collect build logs and installed binaries
      tar czvf "${local_tarball}" \
        "${workdir}/open-gpu-kernel-modules/kernel-open/"*.log \
        $(find /lib/modules/${uname_r}/ -iname 'nvidia*.ko')
      gcloud storage cp "${local_tarball}" "${gcs_tarball}"
      rm "${local_tarball}"
      make clean
      popd
    fi
    gcloud storage cat "${gcs_tarball}" | tar -C / -xzv
    depmod -a
  }

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

  # This .run file contains NV's OpenGL implementation as well as
  # nvidia optimized implementations of the gtk+ 2,3 stack(s) not
  # including glib (https://docs.gtk.org/glib/), and what appears to
  # be a copy of the source from the kernel-open directory of for
  # example DRIVER_VERSION=560.35.03
  #
  # https://github.com/NVIDIA/open-gpu-kernel-modules/archive/refs/tags/560.35.03.tar.gz
  #
  # wget https://us.download.nvidia.com/XFree86/Linux-x86_64/560.35.03/NVIDIA-Linux-x86_64-560.35.03.run
  # sh ./NVIDIA-Linux-x86_64-560.35.03.run -x # this will allow you to review the contents of the package without installing it.
  if test -f "${workdir}/complete/userspace" ; then return ; fi
  local local_fn="${tmpdir}/userspace.run"

  cache_fetched_package "${USERSPACE_URL}" \
                        "${pkg_bucket}/${USERSPACE_FILENAME}" \
                        "${local_fn}"

  local runfile_args
  runfile_args=""
  local cache_hit="0"
  local local_tarball

  if is_rocky8 ; then
    local nvidia_ko_path="$(find /lib/modules/$(uname -r)/ -name 'nvidia.ko')"
    test -n "${nvidia_ko_path}" && test -f "${nvidia_ko_path}" || {
      local build_tarball="kmod_${_shortname}_${DRIVER_VERSION}.tar.gz"
      local_tarball="${workdir}/${build_tarball}"
      local build_dir
      if test -v modulus_md5sum && [[ -n "${modulus_md5sum}" ]]
        then build_dir="${modulus_md5sum}"
        else build_dir="unsigned" ; fi

      local gcs_tarball="${pkg_bucket}/${_shortname}/${uname_r}/${build_dir}/${build_tarball}"

      if gsutil ls "${gcs_tarball}" 2>&1 | grep -q "${gcs_tarball}" ; then
        cache_hit="1"
        runfile_args="--no-kernel-modules"
        echo "cache hit"
      else
        install_build_dependencies

        local signing_options
        signing_options=""
        if [[ -n "${PSN}" ]]; then
          signing_options="--module-signing-hash sha256 \
          --module-signing-x509-hash sha256 \
          --module-signing-secret-key \"${mok_key}\" \
          --module-signing-public-key \"${mok_der}\" \
          --module-signing-script \"/lib/modules/${uname_r}/build/scripts/sign-file\" \
          "
        fi

        runfile_args="--no-dkms ${signing_options}"
      fi
    }
  else
    runfile_args="--no-kernel-modules"
  fi

  execute_with_retries bash "${local_fn}" -e -q \
    ${runfile_args} \
    --ui=none \
    --install-libglvnd \
    --tmpdir="${tmpdir}"

  if is_rocky8 ; then
    if [[ "${cache_hit}" == "1" ]] ; then
      gcloud storage cat "${gcs_tarball}" | tar -C / -xzv
      depmod -a
    else
      tar czvf "${local_tarball}" \
        /var/log/nvidia-installer.log \
        $(find /lib/modules/${uname_r}/ -iname 'nvidia*.ko')
      gcloud storage cp "${local_tarball}" "${gcs_tarball}"
    fi
  fi

  rm -f "${local_fn}"
  touch "${workdir}/complete/userspace"
  sync
}

function install_cuda_runfile() {
  if test -f "${workdir}/complete/cuda" ; then return ; fi
  local local_fn="${tmpdir}/cuda.run"

  cache_fetched_package "${NVIDIA_CUDA_URL}" \
			"${pkg_bucket}/${CUDA_RUNFILE}" \
                        "${local_fn}"

  execute_with_retries bash "${local_fn}" --toolkit --no-opengl-libs --silent --tmpdir="${tmpdir}"
  rm -f "${local_fn}"
  touch "${workdir}/complete/cuda"
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
  elif is_rocky ; then
    # rocky9: cuda-11-[7,8], cuda-12-[1..6]
    execute_with_retries dnf -y -q install "${cudatk_package}"
  fi
  sync
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

function install_cuda(){
  if test -f "${workdir}/complete/cuda-repo" ; then return ; fi

  if ( ge_debian12 && is_src_os ) ; then
    echo "installed with the driver on ${_shortname}"
    return 0
  fi

  # The OS package distributions are unreliable
  install_cuda_runfile

  # Includes CUDA packages
  add_repo_cuda

  touch "${workdir}/complete/cuda-repo"
}

function install_nvidia_container_toolkit() {
  local container_runtime_default
    if command -v docker     ; then container_runtime_default='docker'
  elif command -v containerd ; then container_runtime_default='containerd'
  elif command -v crio       ; then container_runtime_default='crio'
                               else container_runtime_default='' ; fi
  CONTAINER_RUNTIME=$(get_metadata_attribute 'container-runtime' "${container_runtime_default}")

  if test -z "${CONTAINER_RUNTIME}" ; then return ; fi

  add_repo_nvidia_container_toolkit
  if is_debuntu ; then
    execute_with_retries apt-get install -y -q nvidia-container-toolkit ; else
    execute_with_retries dnf     install -y -q nvidia-container-toolkit ; fi
  nvidia-ctk runtime configure --runtime="${CONTAINER_RUNTIME}"
  systemctl restart "${CONTAINER_RUNTIME}"
}

# Install NVIDIA GPU driver provided by NVIDIA
function install_nvidia_gpu_driver() {
  if test -f "${workdir}/complete/gpu-driver" ; then return ; fi

  if ( ge_debian12 && is_src_os ) ; then
    add_nonfree_components
    apt-get update -qq
    apt-get -yq install \
        dkms \
        nvidia-open-kernel-dkms \
        nvidia-open-kernel-support \
        nvidia-smi \
        libglvnd0 \
        libcuda1
    echo "NVIDIA GPU driver provided by ${_shortname} was installed successfully"
    return 0
  fi

  # OS driver packages do not produce reliable driver ; use runfile
  install_nvidia_userspace_runfile

  build_driver_from_github

  echo "NVIDIA GPU driver provided by NVIDIA was installed successfully"
  touch "${workdir}/complete/gpu-driver"
}

function install_ops_agent(){
  if test -f "${workdir}/complete/ops-agent" ; then return ; fi

  mkdir -p /opt/google
  cd /opt/google
  # https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent/installation
  curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
  execute_with_retries bash add-google-cloud-ops-agent-repo.sh --also-install

  touch "${workdir}/complete/ops-agent"
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_monitoring_agent() {
  download_gpu_monitoring_agent
  install_gpu_monitoring_agent_dependency
  start_gpu_monitoring_agent_service
}

function download_gpu_monitoring_agent(){
  if is_rocky ; then
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

function install_gpu_monitoring_agent_dependency(){
  cd /opt/google/compute-gpu-monitoring/linux
  python3 -m venv venv
  venv/bin/pip install wheel
  venv/bin/pip install -Ur requirements.txt
}

function start_gpu_monitoring_agent_service(){
  cp /opt/google/compute-gpu-monitoring/linux/systemd/google_gpu_monitoring_agent_venv.service /lib/systemd/system
  systemctl daemon-reload
  systemctl --no-reload --now enable /lib/systemd/system/google_gpu_monitoring_agent_venv.service
}

# Collects 'gpu_utilization' and 'gpu_memory_utilization' metrics
function install_gpu_agent() {
  # Stackdriver GPU agent parameters
#  local -r GPU_AGENT_REPO_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/master/dlvm/gcp-gpu-utilization-metrics'
  local -r GPU_AGENT_REPO_URL='https://raw.githubusercontent.com/GoogleCloudPlatform/ml-on-gcp/refs/heads/master/dlvm/gcp-gpu-utilization-metrics'
  if ( ! command -v pip && is_debuntu ) ; then
    execute_with_retries "apt-get install -y -qq python3-pip"
  fi
  local install_dir=/opt/gpu-utilization-agent
  mkdir -p "${install_dir}"
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${GPU_AGENT_REPO_URL}/requirements.txt" -o "${install_dir}/requirements.txt"
  curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 \
    "${GPU_AGENT_REPO_URL}/report_gpu_metrics.py" \
    | sed -e 's/-u --format=/--format=/' \
    | dd status=none of="${install_dir}/report_gpu_metrics.py"
  local venv="${install_dir}/venv"
  python3 -m venv "${venv}"
(
  source "${venv}/bin/activate"
  python3 -m pip install --upgrade pip
  execute_with_retries python3 -m pip install -r "${install_dir}/requirements.txt"
)
  sync

  # Generate GPU service.
  cat <<EOF >/lib/systemd/system/gpu-utilization-agent.service
[Unit]
Description=GPU Utilization Metric Agent

[Service]
Type=simple
PIDFile=/run/gpu_agent.pid
ExecStart=/bin/bash --login -c '. ${venv}/bin/activate ; python3 "${install_dir}/report_gpu_metrics.py"'
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

function install_spark_rapids() {
  # Update SPARK RAPIDS config
  readonly DEFAULT_SPARK_RAPIDS_VERSION="24.08.1"
  readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
  readonly DEFAULT_XGBOOST_VERSION="1.7.6" # try 2.1.1
  readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})

  local -r rapids_repo_url='https://repo1.maven.org/maven2/ai/rapids'
  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-spark-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-spark-gpu_2.12-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-gpu_2.12-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
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
  local gpu_count
  gpu_count="$(lspci | grep NVIDIA | wc -l)"
  if version_ge "${gpu_count}" "1" ; then
    local executor_cores
    executor_cores="$(nproc | perl -MPOSIX -pe '$_ = POSIX::floor( $_ * 0.75 ); $_-- if $_ % 2')"
    local executor_memory
    executor_memory_gb="$(awk '/^MemFree/ {print $2}' /proc/meminfo | perl -MPOSIX -pe '$_ *= 0.75; $_ = POSIX::floor( $_ / (1024*1024) )')"
    local task_cpus=2
    local gpu_amount
    gpu_amount="$(echo $executor_cores | perl -pe "\$_ = ( ${gpu_count} / (\$_ / ${task_cpus}) )")"

    cat >>"${spark_defaults_conf}" <<EOF
###### BEGIN : RAPIDS properties for Spark ${SPARK_VERSION} ######
# Rapids Accelerator for Spark can utilize AQE, but when the plan is not finalized,
# query explain output won't show GPU operator, if the user has doubts
# they can uncomment the line before seeing the GPU plan explain;
# having AQE enabled gives user the best performance.
spark.executor.resource.gpu.amount=${gpu_count}
spark.plugins=com.nvidia.spark.SQLPlugin
spark.executor.resource.gpu.discoveryScript=${gpus_resources_script}
spark.executor.cores=${executor_cores}
spark.executor.memory=${executor_memory_gb}G
spark.dynamicAllocation.enabled=false
# please update this config according to your application
spark.task.resource.gpu.amount=${gpu_amount}
spark.task.cpus=2
spark.yarn.unmanagedAM.enabled=false
###### END   : RAPIDS properties for Spark ${SPARK_VERSION} ######
EOF
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
  if   [[ "${nvsmi_works}" == "1" ]] ; then echo -n ''
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

function install_build_dependencies() {
  if test -f "${workdir}/complete/build-dependencies" ; then return ; fi

  if is_debuntu ; then
    if is_ubuntu22 && is_cuda12 ; then
      # On ubuntu22, the default compiler does not build some kernel module versions
      # https://forums.developer.nvidia.com/t/linux-new-kernel-6-5-0-14-ubuntu-22-04-can-not-compile-nvidia-display-card-driver/278553/11
      execute_with_retries apt-get install -y -qq gcc-12
      update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 11
      update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 12
      update-alternatives --set gcc /usr/bin/gcc-12
    fi

  elif is_rocky ; then
    execute_with_retries dnf -y -q install gcc

    local dnf_cmd="dnf -y -q install kernel-devel-${uname_r}"
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
  touch "${workdir}/complete/build-dependencies"
}

function install_dependencies() {
  pkg_list="pciutils screen"
  if is_debuntu ; then execute_with_retries apt-get -y -q install ${pkg_list}
  elif is_rocky ; then execute_with_retries dnf     -y -q install ${pkg_list} ; fi
}

function prepare_gpu_env(){
  # Verify SPARK compatability
  RAPIDS_RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')

  nvsmi_works="0"

  if   is_cuda11 ; then gcc_ver="11"
  elif is_cuda12 ; then gcc_ver="12" ; fi
}

# Hold all NVIDIA-related packages from upgrading unintenionally or services like unattended-upgrades
# Users should run apt-mark unhold before they wish to upgrade these packages
function hold_nvidia_packages() {
  apt-mark hold nvidia-*
  apt-mark hold libnvidia-*
  if dpkg -l | grep -q "xserver-xorg-video-nvidia"; then
    apt-mark hold xserver-xorg-video-nvidia*
  fi
}

function delete_mig_instances() (
  # delete all instances
  set +e
  nvidia-smi mig -dci

  case "${?}" in
    "0" ) echo "compute instances deleted"            ;;
    "2" ) echo "invalid argument"                     ;;
    "6" ) echo "No compute instances found to delete" ;;
    *   ) echo "unrecognized return code"             ;;
  esac

  nvidia-smi mig -dgi
  case "${?}" in
    "0" ) echo "compute instances deleted"        ;;
    "2" ) echo "invalid argument"                 ;;
    "6" ) echo "No GPU instances found to delete" ;;
    *   ) echo "unrecognized return code"         ;;
  esac
)

# https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-operator-mig.html#configuring-mig-profiles
function configure_mig_cgi() {
  delete_mig_instances
  META_MIG_CGI_VALUE="$(get_metadata_attribute 'MIG_CGI')"
  if test -n "${META_MIG_CGI_VALUE}"; then
    nvidia-smi mig -cgi "${META_MIG_CGI_VALUE}" -C
  else
    if lspci | grep -q H100 ; then
      # run the following command to list placement profiles
      # nvidia-smi mig -lgipp
      #
      # This is the result when using H100 instances on 20241220
      # GPU  0 Profile ID 19 Placements: {0,1,2,3,4,5,6}:1
      # GPU  0 Profile ID 20 Placements: {0,1,2,3,4,5,6}:1
      # GPU  0 Profile ID 15 Placements: {0,2,4,6}:2
      # GPU  0 Profile ID 14 Placements: {0,2,4}:2
      # GPU  0 Profile ID  9 Placements: {0,4}:4
      # GPU  0 Profile ID  5 Placement : {0}:4
      # GPU  0 Profile ID  0 Placement : {0}:8

      # For H100 3D controllers, use profile 19, 7x1G instances
      nvidia-smi mig -cgi 19 -C
    elif lspci | grep -q A100 ; then
      # Dataproc only supports A100s right now split in 2 if not specified
      # https://docs.nvidia.com/datacenter/tesla/mig-user-guide/#creating-gpu-instances
      nvidia-smi mig -cgi 9,9 -C
    else
      echo "unrecognized 3D controller"
    fi
  fi
}

function enable_mig() {
  nvidia-smi -mig 1
}

function setup_gpu_yarn() {
  # This configuration should be run on all nodes
  # regardless if they have attached GPUs
  configure_yarn_resources

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
      install_cuda
      load_kernel_module

      #Install GPU metrics collection in Stackdriver if needed
      if [[ "${INSTALL_GPU_AGENT}" == "true" ]]; then
        install_gpu_agent
#        install_gpu_monitoring_agent
        echo 'GPU metrics agent successfully deployed.'
      else
        echo 'GPU metrics agent has not been installed.'
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
}


function main() {
  setup_gpu_yarn
  if [[ -n ${CUDNN_VERSION} ]]; then
    install_nvidia_nccl
    install_nvidia_cudnn
  fi
  install_nvidia_container_toolkit

  if [[ "${RAPIDS_RUNTIME}" == "SPARK" ]]; then
    install_spark_rapids
    configure_gpu_script
    echo "RAPIDS initialized with Spark runtime"
  elif [[ "${RAPIDS_RUNTIME}" == "DASK" ]]; then
    # we are not currently tooled for installing dask in this action.
    echo "RAPIDS recognizes DASK runtime - currently supported using dask/dask.sh or rapids/rapids.sh"
  else
    echo "Unrecognized RAPIDS Runtime: ${RAPIDS_RUNTIME}"
  fi

  # Restart YARN services if they are running already
  for svc in resourcemanager nodemanager; do
    if [[ "$(systemctl show hadoop-yarn-${svc}.service -p SubState --value)" == 'running' ]]; then
      systemctl restart "hadoop-yarn-${svc}.service"
    fi
  done
}

function exit_handler() {
  # Purge private key material until next grant
  clear_dkms_key

  set +ex
  echo "Exit handler invoked"

  # Clear pip cache
  pip cache purge || echo "unable to purge pip cache"

  # If system memory was sufficient to mount memory-backed filesystems
  if [[ "${tmpdir}" == "/mnt/shm" ]] ; then
    # remove the tmpfs pip cache-dir
    pip config unset global.cache-dir || echo "unable to unset global pip cache"

    # Clean up shared memory mounts
    for shmdir in /var/cache/apt/archives /var/cache/dnf /mnt/shm /tmp /var/cudnn-local ; do
      if ( grep -q "^tmpfs ${shmdir}" /proc/mounts && ! grep -q "^tmpfs ${shmdir}" /etc/fstab ) ; then
        umount -f ${shmdir}
      fi
    done

    # restart services stopped during preparation stage
    # systemctl list-units | perl -n -e 'qx(systemctl start $1) if /^.*? ((hadoop|knox|hive|mapred|yarn|hdfs)\S*).service/'
  fi

  if is_debuntu ; then
    # Clean up OS package cache
    apt-get -y -qq clean
    apt-get -y -qq -o DPkg::Lock::Timeout=60 autoremove
    # re-hold systemd package
    if ge_debian12 ; then
    apt-mark hold systemd libsystemd0 ; fi
    hold_nvidia_packages
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
    du -x -hs \
      /usr/lib/{pig,hive,hadoop,jvm,spark,google-cloud-sdk,x86_64-linux-gnu} \
      /var/lib/{docker,mysql,} \
      /usr/lib \
      /opt/nvidia/* \
      /usr/local/cuda-1?.? \
      /opt/{conda,google-cloud-ops-agent,install-nvidia,} \
      /usr/bin \
      /usr \
      /var \
      / 2>/dev/null | sort -h
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

function prepare_to_install(){
  # Verify OS compatability and Secure boot state
  check_os
  check_secure_boot

  prepare_gpu_env

  OS_NAME="$(lsb_release -is | tr '[:upper:]' '[:lower:]')"
  readonly OS_NAME

  # node role
  ROLE="$(get_metadata_attribute dataproc-role)"
  readonly ROLE

  workdir=/opt/install-dpgce
  tmpdir=/tmp/
  temp_bucket="$(get_metadata_attribute dataproc-temp-bucket)"
  readonly temp_bucket
  readonly pkg_bucket="gs://${temp_bucket}/dpgce-packages"
  uname_r=$(uname -r)
  readonly uname_r
  readonly bdcfg="/usr/local/bin/bdconfig"
  export DEBIAN_FRONTEND=noninteractive

  mkdir -p "${workdir}/complete"
  trap exit_handler EXIT
  set_proxy
  mount_ramdisk

  readonly install_log="${tmpdir}/install.log"

  if test -f "${workdir}/complete/prepare" ; then return ; fi

  repair_old_backports

  if is_debuntu ; then
    clean_up_sources_lists
    apt-get update -qq
    apt-get -y clean
    apt-get -o DPkg::Lock::Timeout=60 -y autoremove
    if ge_debian12 ; then
    apt-mark unhold systemd libsystemd0 ; fi
  else
    dnf clean all
  fi

  # zero free disk space
  if [[ -n "$(get_metadata_attribute creating-image)" ]]; then ( set +e
    time dd if=/dev/zero of=/zero status=none ; sync ; sleep 3s ; rm -f /zero
  ) fi

  install_dependencies

  # Monitor disk usage in a screen session
  df / > "/run/disk-usage.log"
  touch "/run/keep-running-df"
  screen -d -m -LUS keep-running-df \
    bash -c "while [[ -f /run/keep-running-df ]] ; do df / | tee -a /run/disk-usage.log ; sleep 5s ; done"

  touch "${workdir}/complete/prepare"
}

prepare_to_install

main
