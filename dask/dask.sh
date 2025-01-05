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
# initialization-actions/templates/dask/dask.sh.in
#
# Modifications made directly to the generated file will be lost when
# the template is re-evaluated

#
# This initialization action script will install Dask and other relevant
# libraries on a Dataproc cluster. This is supported for either "yarn" or
# "standalone" runtimes Please see dask.org and yarn.dask.org for more
# information.

set -euxo pipefail

function os_id()       ( set +x ;  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_version()  ( set +x ;  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_codename() ( set +x ;  grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs ; )

# For version (or real number) comparison
# if first argument is greater than or equal to, greater than, less than or equal to, or less than the second
# ( version_ge 2.0 2.1 ) evaluates to false
# ( version_ge 2.2 2.1 ) evaluates to true
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
  eval "function is_debuntu()  ( set +x ;  is_debian || is_ubuntu ; )"
}

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

  while ! command -v gcloud ; do sleep 5s ; done

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
    echo "Error: Secure Boot is not supported on Debian before image 2.2. Consider disabling Secure Boot while creating the cluster."
    return
  elif [[ "${SECURE_BOOT}" == "enabled" ]] && [[ -z "${PSN}" ]]; then
    echo "Secure boot is enabled, but no signing material provided."
    echo "Consider either disabling secure boot or provide signing material as per"
    echo "https://github.com/GoogleCloudDataproc/custom-images/tree/master/examples/secure-boot"
    return
  fi

  CA_TMPDIR="$(mktemp -u -d -p /run/tmp -t ca_dir-XXXX)"
  readonly CA_TMPDIR

  if is_ubuntu ; then mok_key=/var/lib/shim-signed/mok/MOK.priv
                      mok_der=/var/lib/shim-signed/mok/MOK.der
                 else mok_key=/var/lib/dkms/mok.key
                      mok_der=/var/lib/dkms/mok.pub ; fi
}

function restart_knox() {
  systemctl stop knox
  rm -rf "${KNOX_HOME}/data/deployments/*"
  systemctl start knox
}

function install_dependencies() {
  test -f "${workdir}/complete/install-dependencies" && return 0
  pkg_list="screen"
  if is_debuntu ; then execute_with_retries apt-get -y -q install ${pkg_list}
  elif is_rocky ; then execute_with_retries dnf     -y -q install ${pkg_list} ; fi
  touch "${workdir}/complete/install-dependencies"
}

function prepare_common_env() {
  define_os_comparison_functions

  # Verify OS compatability and Secure boot state
  check_os
  check_secure_boot

  readonly _shortname="$(os_id)$(os_version|perl -pe 's/(\d+).*/$1/')"

  # Dataproc configurations
  readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
  readonly HIVE_CONF_DIR='/etc/hive/conf'
  readonly SPARK_CONF_DIR='/etc/spark/conf'

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

  # Knox config
  readonly KNOX_HOME=/usr/lib/knox
  readonly KNOX_DASK_DIR="${KNOX_HOME}/data/services/dask/0.1.0"
  readonly KNOX_DASKWS_DIR="${KNOX_HOME}/data/services/daskws/0.1.0"

  mkdir -p "${workdir}/complete"
  set_proxy
  mount_ramdisk

  readonly install_log="${tmpdir}/install.log"

  if test -f "${workdir}/complete/prepare.common" ; then return ; fi

  repair_old_backports

  if is_debuntu ; then
    clean_up_sources_lists
    apt-get update -qq
    apt-get -y clean
    apt-get -o DPkg::Lock::Timeout=60 -y autoremove
    if ge_debian12 ; then
    apt-mark unhold systemd libsystemd0 ; fi
    if is_ubuntu ; then
      while ! command -v gcloud ; do sleep 5s ; done
    fi
  else
    dnf clean all
  fi

  # zero free disk space
  if [[ -n "$(get_metadata_attribute creating-image)" ]]; then

 ( set +e
    time dd if=/dev/zero of=/zero status=none ; sync ; sleep 3s ; rm -f /zero
  )

    install_dependencies

    # Monitor disk usage in a screen session
    df / > "/run/disk-usage.log"
    touch "/run/keep-running-df"
    screen -d -m -LUS keep-running-df \
      bash -c "while [[ -f /run/keep-running-df ]] ; do df / | tee -a /run/disk-usage.log ; sleep 5s ; done"
 fi

  touch "${workdir}/complete/prepare.common"
}

function common_exit_handler() {
  set +ex
  echo "Exit handler invoked"

  # Clear pip cache
  pip cache purge || echo "unable to purge pip cache"

  # Restart YARN services if they are running already
  for svc in resourcemanager nodemanager; do
    if [[ "$(systemctl show hadoop-yarn-${svc}.service -p SubState --value)" == 'running' ]]; then
      systemctl  stop "hadoop-yarn-${svc}.service"
      systemctl start "hadoop-yarn-${svc}.service"
    fi
  done

  # If system memory was sufficient to mount memory-backed filesystems
  if [[ "${tmpdir}" == "/mnt/shm" ]] ; then
    # remove the tmpfs pip cache-dir
    pip config unset global.cache-dir || echo "unable to unset global pip cache"

    # Clean up shared memory mounts
    for shmdir in /var/cache/apt/archives /var/cache/dnf /mnt/shm /tmp ; do
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

  if [[ -n "$(get_metadata_attribute creating-image)" ]]; then
    # print disk usage statistics for large components
    if is_ubuntu ; then
      du -hs \
        /usr/lib/{pig,hive,hadoop,jvm,spark,google-cloud-sdk,x86_64-linux-gnu} \
        /usr/lib \
        /opt/nvidia/* \
        /opt/conda/miniconda3 | sort -h
    elif is_debian ; then
      du -x -hs \
        /usr/lib/{pig,hive,hadoop,jvm,spark,google-cloud-sdk,x86_64-linux-gnu,} \
        /var/lib/{docker,mysql,} \
        /opt/nvidia/* \
        /opt/{conda,google-cloud-ops-agent,install-nvidia,} \
        /usr/bin \
        /usr \
        /var \
        / 2>/dev/null | sort -h
    else
      du -hs \
        /var/lib/docker \
        /usr/lib/{pig,hive,hadoop,firmware,jvm,spark,atlas,} \
        /usr/lib64/google-cloud-sdk \
        /opt/nvidia/* \
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

    perl -e \
          '@siz=( sort { $a => $b }
                   map { (split)[2] =~ /^(\d+)/ }
                  grep { m:^/: } <STDIN> );
$max=$siz[0]; $min=$siz[-1]; $inc=$max-$min;
print( "    samples-taken: ", scalar @siz, $/,
       "maximum-disk-used: $max", $/,
       "minimum-disk-used: $min", $/,
       "     increased-by: $inc", $/ )' < "/run/disk-usage.log"


    # zero free disk space
    dd if=/dev/zero of=/zero
    sync
    sleep 3s
    rm -f /zero
  fi
  echo "exit_handler has completed"
}


function configure_dask_yarn() {
  readonly DASK_YARN_CONFIG_DIR=/etc/dask/
  readonly DASK_YARN_CONFIG_FILE=${DASK_YARN_CONFIG_DIR}/config.yaml
  # Minimal custom configuration is required for this
  # setup. Please see https://yarn.dask.org/en/latest/quickstart.html#usage
  # for information on tuning Dask-Yarn environments.
  mkdir -p "${DASK_YARN_CONFIG_DIR}"

  local worker_class="dask.distributed.Nanny"
  local gpu_count="0"
  if command -v nvidia-smi ; then
    gpu_count="1"
    worker_class="dask_cuda.CUDAWorker"
  fi

  cat <<EOF >"${DASK_YARN_CONFIG_FILE}"
# Config file for Dask Yarn.
#
# These values are joined on top of the default config, found at
# https://yarn.dask.org/en/latest/configuration.html#default-configuration

yarn:
  environment: python://${DASK_CONDA_ENV}/bin/python

  worker:
    count: 2
    gpus: ${gpu_count}
    worker_class: ${worker_class}
EOF
}

function install_systemd_dask_worker() {
  echo "Installing systemd Dask Worker service..."
  local -r dask_worker_local_dir="/tmp/${DASK_WORKER_SERVICE}"

  mkdir -p "${dask_worker_local_dir}"

  local DASK_WORKER_LAUNCHER="/usr/local/bin/${DASK_WORKER_SERVICE}-launcher.sh"

  local compute_mode_cmd=""
  if command -v nvidia-smi ; then compute_mode_cmd="nvidia-smi --compute-mode=DEFAULT" ; fi
  local worker_name="dask-worker"
  if test -f "${DASK_CONDA_ENV}/bin/dask-cuda-worker" ; then worker_name="dask-cuda-worker" ; fi
  local worker="${DASK_CONDA_ENV}/bin/${worker_name}"
  cat <<EOF >"${DASK_WORKER_LAUNCHER}"
#!/bin/bash
LOGFILE="/var/log/${DASK_WORKER_SERVICE}.log"
${compute_mode_cmd}
echo "${worker_name} starting, logging to \${LOGFILE}"
${worker} "${MASTER}:8786" --local-directory="${dask_worker_local_dir}" --memory-limit=auto >> "\${LOGFILE}" 2>&1
EOF

  chmod 750 "${DASK_WORKER_LAUNCHER}"

  local -r dask_service_file="/usr/lib/systemd/system/${DASK_WORKER_SERVICE}.service"
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Worker Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_WORKER_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload

  # Enable the service
  enable_systemd_dask_worker_service="0"
  if [[ "${ROLE}" != "Master" ]]; then
    enable_systemd_dask_worker_service="1"
  else
    # Enable service on single-node cluster (no workers)
    local worker_count="$(get_metadata_attribute dataproc-worker-count)"
    if [[ "${worker_count}" == "0" ]] &&
       [[ "$(get_metadata_attribute dask-cuda-worker-on-master 'true')" == "true" ]] &&
       [[ "$(get_metadata_attribute dask-worker-on-master 'true')" == "true" ]] ; then
      enable_systemd_dask_worker_service="1"
    fi
  fi
  readonly enable_systemd_dask_worker_service

  if [[ "${enable_systemd_dask_worker_service}" == "1" ]]; then
    systemctl enable "${DASK_WORKER_SERVICE}"
    systemctl restart "${DASK_WORKER_SERVICE}"
  fi
}

function install_systemd_dask_scheduler() {
  # only run scheduler on primary master
  if [[ "$(hostname -s)" != "${MASTER}" ]]; then return ; fi
  echo "Installing systemd Dask Scheduler service..."
  local -r dask_scheduler_local_dir="/tmp/${DASK_SCHEDULER_SERVICE}"

  mkdir -p "${dask_scheduler_local_dir}"

  local DASK_SCHEDULER_LAUNCHER="/usr/local/bin/${DASK_SCHEDULER_SERVICE}-launcher.sh"

  cat <<EOF >"${DASK_SCHEDULER_LAUNCHER}"
#!/bin/bash
LOGFILE="/var/log/${DASK_SCHEDULER_SERVICE}.log"
echo "dask scheduler starting, logging to \${LOGFILE}"
${DASK_CONDA_ENV}/bin/dask scheduler >> "\${LOGFILE}" 2>&1
EOF

  chmod 750 "${DASK_SCHEDULER_LAUNCHER}"

  local -r dask_service_file="/usr/lib/systemd/system/${DASK_SCHEDULER_SERVICE}.service"
  cat <<EOF >"${dask_service_file}"
[Unit]
Description=Dask Scheduler Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_SCHEDULER_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF
  chmod a+r "${dask_service_file}"

  systemctl daemon-reload

  # Enable the service
  systemctl enable "${DASK_SCHEDULER_SERVICE}"
}

function install_systemd_dask_service() {
  install_systemd_dask_scheduler
  install_systemd_dask_worker
}

function configure_knox_for_dask() {
  if [[ ! -d "${KNOX_HOME}" ]]; then
    echo "Skip configuring Knox rules for Dask"
    return 0
  fi

  local DASK_UI_PORT=8787
  if [[ -f /etc/knox/conf/topologies/default.xml ]]; then
    sed -i \
      "/<\/topology>/i <service><role>DASK<\/role><url>http://localhost:${DASK_UI_PORT}<\/url><\/service> <service><role>DASKWS<\/role><url>ws:\/\/${MASTER}:${DASK_UI_PORT}<\/url><\/service>" \
      /etc/knox/conf/topologies/default.xml
  fi

  mkdir -p "${KNOX_DASK_DIR}"

  cat >"${KNOX_DASK_DIR}/service.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<service role="DASK" name="dask" version="0.1.0">
  <policies>
    <policy role="webappsec"/>
    <policy role="authentication" name="Anonymous"/>
    <policy role="rewrite"/>
    <policy role="authorization"/>
  </policies>

  <routes>
    <!-- Javascript paths -->
    <route path="/dask/**/*.js">
      <rewrite apply="DASK/dask/inbound/js/dask" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/js" to="response.body"/>
    </route>
    <route path="/dask/**/*.js?**">
      <rewrite apply="DASK/dask/inbound/js/dask" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/js" to="response.body"/>
    </route>

    <!-- CSS paths -->
    <route path="/dask/**/*.css">
      <rewrite apply="DASK/dask/inbound/css/dask" to="request.url"/>
    </route>

    <!-- General path routing -->
    <route path="/dask">
      <rewrite apply="DASK/dask/inbound/root" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/headers" to="response.headers"/>
    </route>
    <route path="/dask/**">
      <rewrite apply="DASK/dask/inbound/root/path" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/headers" to="response.headers"/>
      <rewrite apply="DASK/dask/outbound/logs" to="response.body"/>
    </route>
    <route path="/dask/**?**">
      <rewrite apply="DASK/dask/inbound/root/query" to="request.url"/>
      <rewrite apply="DASK/dask/outbound/headers" to="response.headers"/>
      <rewrite apply="DASK/dask/outbound/logs" to="response.body"/>
    </route>
  </routes>
  <dispatch classname="org.apache.knox.gateway.dispatch.PassAllHeadersNoChunkedPostDispatch"/>
</service>
EOF

  cat >"${KNOX_DASK_DIR}/rewrite.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<rules>
  <rule dir="IN" name="DASK/dask/inbound/js/dask" pattern="http://*:*/**/dask/{**}?{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}?{**}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/root" pattern="http://*:*/**/dask">
    <rewrite template="{$serviceUrl[DASK]}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/root/path" pattern="http://*:*/**/dask/{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/root/query" pattern="http://*:*/**/dask/{**}?{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}?{**}"/>
  </rule>
  <rule dir="IN" name="DASK/dask/inbound/css/dask" pattern="http://*:*/**/dask/{**}?{**}">
    <rewrite template="{$serviceUrl[DASK]}/{**}?{**}"/>
  </rule>
  <!-- without the /gateway/default prefix -->
  <rule dir="IN" name="DASK/dask/inbound/root/noprefix" pattern="http://*:*/dask">
    <rewrite template="{$serviceUrl[DASK]}"/>
  </rule>

  <rule dir="OUT" name="DASK/dask/outbound/logs" pattern="/logs">
    <rewrite template="{$frontend[path]}/dask/info/logs"/>
  </rule>

  <!-- Rewrite redirect responses Location header -->
  <filter name="DASK/dask/outbound/headers">
    <content type="application/x-http-headers">
      <apply path="Location" rule="DASK/dask/outbound/headers/location"/>
    </content>
  </filter>

  <rule dir="OUT" name="DASK/dask/outbound/headers/location" flow="OR">
    <match pattern="*://*:*/">
      <rewrite template="{$frontend[path]}/dask/"/>
    </match>
    <match pattern="*://*:*/{**}">
      <rewrite template="{$frontend[path]}/dask/{**}"/>
    </match>
    <match pattern="*://*:*/{**}?{**}">
      <rewrite template="{$frontend[path]}/dask/{**}?{**}"/>
    </match>
    <match pattern="/{**}">
      <rewrite template="{$frontend[path]}/dask/{**}"/>
    </match>
    <match pattern="/{**}?{**}">
      <rewrite template="{$frontend[path]}/dask/{**}?{**}"/>
    </match>
  </rule>
</rules>
EOF

  mkdir -p "${KNOX_DASKWS_DIR}"

  cat >"${KNOX_DASKWS_DIR}/service.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<service role="DASKWS" name="daskws" version="0.1.0">
  <policies>
    <policy role="webappsec"/>
    <policy role="authentication" name="Anonymous"/>
    <policy role="rewrite"/>
    <policy role="authorization"/>
  </policies>

  <routes>

    <route path="/dask/**/ws">
      <rewrite apply="DASKWS/daskws/inbound/ws" to="request.url"/>
    </route>

  </routes>
  <dispatch classname="org.apache.knox.gateway.dispatch.PassAllHeadersNoChunkedPostDispatch"/>
</service>
EOF

  cat >"${KNOX_DASKWS_DIR}/rewrite.xml" <<'EOF'
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>

<rules>
  <rule dir="IN" name="DASKWS/daskws/inbound/ws" pattern="ws://*:*/**/dask/{**}/ws">
    <rewrite template="{$serviceUrl[DASKWS]}/{**}/ws"/>
  </rule>
</rules>
EOF

  chown -R knox:knox "${KNOX_DASK_DIR}" "${KNOX_DASKWS_DIR}"

  # Do not restart knox during pre-init script run
  if [[ -n "${ROLE}" ]]; then
    restart_knox
  fi
}

function configure_fluentd_for_dask() {
  if [[ "$(hostname -s)" == "${MASTER}" ]]; then
    cat >/etc/google-fluentd/config.d/dataproc-dask.conf <<EOF
# Fluentd config for Dask logs

# Dask scheduler
<source>
  @type tail
  path /var/log/dask-scheduler.log
  pos_file /var/tmp/fluentd.dataproc.dask.scheduler.pos
  read_from_head true
  tag google.dataproc.dask-scheduler
  <parse>
    @type none
  </parse>
</source>

<filter google.dataproc.dask-scheduler>
  @type record_transformer
  <record>
    filename dask-scheduler.log
  </record>
</filter>
EOF
  fi

  if [[ "${enable_systemd_dask_worker_service}" == "1" ]]; then
    cat >>/etc/google-fluentd/config.d/dataproc-dask.conf <<EOF
# Dask worker
<source>
  @type tail
  path /var/log/dask-worker.log
  pos_file /var/tmp/fluentd.dataproc.dask.worker.pos
  read_from_head true
  tag google.dataproc.dask-worker
  <parse>
    @type none
  </parse>
</source>

<filter google.dataproc.dask-worker>
  @type record_transformer
  <record>
    filename dask-worker.log
  </record>
</filter>
EOF
  fi

  systemctl restart google-fluentd
}

function install_dask() {
  local python_spec="python>=3.11"
  local dask_spec="dask>=2024.7"

  CONDA_PACKAGES=()
  if [[ "${DASK_RUNTIME}" == 'yarn' ]]; then
    # Pin `distributed` and `dask` package versions to old release
    # because `dask-yarn` 0.9 uses skein in a way which
    # is not compatible with `distributed` package 2022.2 and newer:
    # https://github.com/dask/dask-yarn/issues/155

    dask_spec="dask<2022.2"
    python_spec="python>=3.7,<3.8.0a0"
    if is_ubuntu18 ; then
      # the libuuid.so.1 distributed with fiona 1.8.22 dumps core when calling uuid_generate_time_generic
      CONDA_PACKAGES+=("fiona<1.8.22")
    fi
    CONDA_PACKAGES+=('dask-yarn=0.9' "distributed<2022.2")
  fi

  CONDA_PACKAGES+=(
    "${dask_spec}"
    "dask-bigquery"
    "dask-ml"
    "dask-sql"
  )

  # Install dask
  mamba="/opt/conda/miniconda3/bin/mamba"
  conda="/opt/conda/miniconda3/bin/conda"

  ( set +e
  local is_installed=0
  for installer in "${mamba}" "${conda}" ; do
    test -d "${DASK_CONDA_ENV}" || \
      time "${installer}" "create" -m -n "${conda_env}" -y --no-channel-priority \
      -c 'conda-forge' -c 'nvidia'  \
      ${CONDA_PACKAGES[*]} \
      "${python_spec}" \
      > "${install_log}" 2>&1 && retval=$? || { retval=$? ; cat "${install_log}" ; }
    sync
    if [[ "$retval" == "0" ]] ; then
      is_installed="1"
      break
    fi
    "${conda}" config --set channel_priority flexible
  done
  if [[ "${is_installed}" == "0" ]]; then
    echo "failed to install dask"
    return 1
  fi
  )
}

function install_dask_rapids() {
  if is_cuda12 ; then
    local python_spec="python>=3.11"
    local cuda_spec="cuda-version>=12,<13"
    local dask_spec="dask>=2024.7"
    local numba_spec="numba"
  elif is_cuda11 ; then
    local python_spec="python>=3.9"
    local cuda_spec="cuda-version>=11,<12.0a0"
    local dask_spec="dask"
    local numba_spec="numba"
  fi

  rapids_spec="rapids>=${RAPIDS_VERSION}"
  CONDA_PACKAGES=()
  if [[ "${DASK_RUNTIME}" == 'yarn' ]]; then
    # Pin `distributed` and `dask` package versions to old release
    # because `dask-yarn` 0.9 uses skein in a way which
    # is not compatible with `distributed` package 2022.2 and newer:
    # https://github.com/dask/dask-yarn/issues/155

    dask_spec="dask<2022.2"
    python_spec="python>=3.7,<3.8.0a0"
    rapids_spec="rapids<=24.05"
    if is_ubuntu18 ; then
      # the libuuid.so.1 distributed with fiona 1.8.22 dumps core when calling uuid_generate_time_generic
      CONDA_PACKAGES+=("fiona<1.8.22")
    fi
    CONDA_PACKAGES+=('dask-yarn=0.9' "distributed<2022.2")
  fi

  CONDA_PACKAGES+=(
    "${cuda_spec}"
    "${rapids_spec}"
    "${dask_spec}"
    "dask-bigquery"
    "dask-ml"
    "dask-sql"
    "cudf"
    "${numba_spec}"
  )

  # Install cuda, rapids, dask
  mamba="/opt/conda/miniconda3/bin/mamba"
  conda="/opt/conda/miniconda3/bin/conda"

  ( set +e
  local is_installed="0"
  for installer in "${mamba}" "${conda}" ; do
    test -d "${DASK_CONDA_ENV}" || \
      time "${installer}" "create" -m -n "${conda_env}" -y --no-channel-priority \
      -c 'conda-forge' -c 'nvidia' -c 'rapidsai'  \
      ${CONDA_PACKAGES[*]} \
      "${python_spec}" \
      > "${install_log}" 2>&1 && retval=$? || { retval=$? ; cat "${install_log}" ; }
    sync
    if [[ "$retval" == "0" ]] ; then
      is_installed="1"
      break
    fi
    "${conda}" config --set channel_priority flexible
  done
  if [[ "${is_installed}" == "0" ]]; then
    echo "failed to install dask"
    return 1
  fi
  )
}

function prepare_dask_env() {
  # Dask config
  DASK_RUNTIME="$(get_metadata_attribute dask-runtime || echo 'standalone')"
  readonly DASK_RUNTIME
  readonly DASK_SERVICE=dask-cluster
  readonly DASK_WORKER_SERVICE=dask-worker
  readonly DASK_SCHEDULER_SERVICE=dask-scheduler
  readonly DASK_CONDA_ENV="/opt/conda/miniconda3/envs/${conda_env}"
}

function prepare_dask_rapids_env(){
  prepare_dask_env
  # RAPIDS config
  RAPIDS_RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'DASK')
  readonly RAPIDS_RUNTIME

  local DEFAULT_DASK_RAPIDS_VERSION="24.08"
  if [[ "${DATAPROC_IMAGE_VERSION}" == "2.0" ]] ; then
    DEFAULT_DASK_RAPIDS_VERSION="23.08" # Final release to support spark 3.1.3
  fi
  readonly RAPIDS_VERSION=$(get_metadata_attribute 'rapids-version' ${DEFAULT_DASK_RAPIDS_VERSION})
}


function dask_exit_handler() {
  echo "no exit handler for dask"
}


function main() {
  # Install Dask
  install_dask

  # In "standalone" mode, Dask relies on a systemd unit to launch.
  # In "yarn" mode, it relies on a config.yaml file.
  if [[ "${DASK_RUNTIME}" == "yarn" ]]; then
    # Create Dask YARN config file
    configure_dask_yarn
  elif [[ "${DASK_RUNTIME}" == "standalone" ]]; then
    # Create Dask service
    install_systemd_dask_service

    if [[ "$(hostname -s)" == "${MASTER}" ]]; then
      systemctl start "${DASK_SCHEDULER_SERVICE}"
      systemctl status "${DASK_SCHEDULER_SERVICE}"
    fi

    echo "Starting Dask 'standalone' cluster..."
    if [[ "${enable_worker_service}" == "1" ]]; then
      systemctl start "${DASK_WORKER_SERVICE}"
      systemctl status "${DASK_WORKER_SERVICE}"
    fi

    configure_knox_for_dask

    local DASK_CLOUD_LOGGING="$(get_metadata_attribute dask-cloud-logging || echo 'false')"
    if [[ "${DASK_CLOUD_LOGGING}" == "true" ]]; then
      configure_fluentd_for_dask
    fi
  else
    echo "Unsupported Dask Runtime: ${DASK_RUNTIME}"
    exit 1
  fi

  echo "Dask for ${DASK_RUNTIME} successfully initialized."
}

function exit_handler() {
  gpu_exit_handler
  common_exit_handler
  return 0
}

function prepare_to_install(){
  prepare_common_env
  prepare_gpu_env
  conda_env="$(get_metadata_attribute conda-env || echo 'dask')"
  readonly conda_env
  prepare_dask_env
  trap exit_handler EXIT
}

prepare_to_install

main
