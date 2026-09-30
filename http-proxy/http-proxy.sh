#!/bin/bash

set -euo pipefail

# --- Metadata Helpers ---
function print_metadata_value() {
  local readonly tmpfile=$(mktemp)
  # Capture stdout and http_code separately
  http_code=$(curl -fsSL --retry-connrefused --retry 10 --retry-max-time 30 "${1}" -H "Metadata-Flavor: Google" -w '%{http_code}' \
    -s -o ${tmpfile} --connect-timeout 5 --max-time 10)
  local readonly return_code=$?
  # If the command completed successfully, print the metadata value to stdout.
  if [[ ${return_code} == 0 && "${http_code}" == "200" ]]; then
    cat ${tmpfile}
  fi
  rm -f ${tmpfile}
  return ${return_code}
}

function print_metadata_value_if_exists() {
  local return_code=1
  local readonly url=$1
  print_metadata_value "${url}"
  return_code=$?
  return ${return_code}
}

# replicates /usr/share/google/get_metadata_value
function get_metadata_value() {
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
}

function get_metadata_attribute() {
  local -r attribute_name="$1"
  local -r default_value="${2:-}"
  set +e
  get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
  set -e
}
# --- End Metadata Helpers ---

# --- OS Detection Helpers ---
function os_id()       { grep '^ID='               /etc/os-release | cut -d= -f2 | xargs ; }
function is_debuntu()  {  [[ "$(os_id)" == "debian" || "$(os_id)" == "ubuntu" ]] ; }
function is_rocky()    {  [[ "$(os_id)" == "rocky" ]] ; }
# --- End OS Detection Helpers ---

# --- Version Comparison Helpers ---
function version_le(){ [[ "$1" = "$(echo -e "$1\n$2"|sort -V|head -n1)" ]]; }
function version_lt(){ [[ "$1" = "$2" ]] && return 1 || version_le "$1" "$2"; }
# --- End Version Comparison Helpers ---

function execute_with_retries() {
  local -r cmd="$*"
  local retries=3
  local delay=5
  for ((i = 0; i < retries; i++)); do
    eval "${cmd}" && return 0
    echo "Command failed. Retrying in ${delay} seconds..." >&2
    sleep "${delay}"
  done
  echo "Command failed after ${retries} retries: ${cmd}" >&2
  return 1
}

function set_proxy(){
  # Idempotency Check for Proxy
  if grep -q "http_proxy=" /etc/environment && [[ -n "${http_proxy:-}" ]]; then
    echo "INFO: Proxy already configured in /etc/environment. Skipping proxy setup portion."
    return 0
  fi

  local meta_http_proxy meta_https_proxy meta_proxy_uri
  meta_http_proxy=$(get_metadata_attribute 'http-proxy' '')
  meta_https_proxy=$(get_metadata_attribute 'https-proxy' '')
  meta_proxy_uri=$(get_metadata_attribute 'proxy-uri' '')

  echo "DEBUG: set_proxy: meta_http_proxy='${meta_http_proxy}'"
  echo "DEBUG: set_proxy: meta_https_proxy='${meta_https_proxy}'"
  echo "DEBUG: set_proxy: meta_proxy_uri='${meta_proxy_uri}'"

  local http_proxy_val=""
  local https_proxy_val=""

  # Determine HTTP_PROXY value
  if [[ -n "${meta_http_proxy}" ]] && [[ "${meta_http_proxy}" != ":" ]]; then
    http_proxy_val="${meta_http_proxy}"
  elif [[ -n "${meta_proxy_uri}" ]] && [[ "${meta_proxy_uri}" != ":" ]]; then
    http_proxy_val="${meta_proxy_uri}"
  fi

  # Determine HTTPS_PROXY value
  if [[ -n "${meta_https_proxy}" ]] && [[ "${meta_https_proxy}" != ":" ]]; then
    https_proxy_val="${meta_https_proxy}"
  elif [[ -n "${meta_proxy_uri}" ]] && [[ "${meta_proxy_uri}" != ":" ]]; then
    https_proxy_val="${meta_proxy_uri}"
  fi

  if [[ -z "${http_proxy_val}" && -z "${https_proxy_val}" ]]; then
    echo "DEBUG: set_proxy: No valid proxy metadata found. Skipping proxy config."
    return 0
  fi

  local default_no_proxy_list=(
    "localhost"
    "127.0.0.1"
    "::1"
    "metadata.google.internal"
    "169.254.169.254"
    ".google.com"
    ".googleapis.com"
    ".internal"
  )

  # Add project-specific internal domain
  local project_id
  project_id=$(get_metadata_attribute 'project-id' "${PROJECT_ID:-}")
  if [[ -n "${project_id}" ]]; then
    default_no_proxy_list+=( ".c.${project_id}.internal" )
  fi

  # Add cluster-specific hostnames
  local cluster_name
  cluster_name=$(get_metadata_attribute 'dataproc-cluster-name' '')
  if [[ -n "${cluster_name}" ]]; then
    # Add wildcard patterns (supported by some tools like Go/Java)
    default_no_proxy_list+=( "${cluster_name}-m" "${cluster_name}-m-*" "${cluster_name}-w-*" "${cluster_name}-sw-*" )
    # Add FQDN suffixes to ensure bypass for tools like curl/wget
    default_no_proxy_list+=( "${cluster_name}-m.c.${project_id}.internal" )
    default_no_proxy_list+=( ".c.${project_id}.internal" )
  fi

  local user_no_proxy
  user_no_proxy=$(get_metadata_attribute 'no-proxy' '')
  local user_no_proxy_list=()
  if [[ -n "${user_no_proxy}" ]]; then
    IFS=',' read -r -a user_no_proxy_list <<< "${user_no_proxy//[[:space:]]/}"
  fi

  local combined_no_proxy_list=( "${default_no_proxy_list[@]}" "${user_no_proxy_list[@]}" )
  local no_proxy
  no_proxy=$( IFS=',' ; echo "${combined_no_proxy_list[*]}" )
  export NO_PROXY="${no_proxy}"
  export no_proxy="${no_proxy}"

  # Export environment variables
  if [[ -n "${http_proxy_val}" ]]; then
    export HTTP_PROXY="http://${http_proxy_val}"
    export http_proxy="http://${http_proxy_val}"
  fi
  if [[ -n "${https_proxy_val}" ]]; then
    export HTTPS_PROXY="http://${https_proxy_val}"
    export https_proxy="http://${https_proxy_val}"
  fi

  # Clear existing proxy settings in /etc/environment
  sed -i -e '/^http_proxy=/d' -e '/^https_proxy=/d' -e '/^no_proxy=/d' \
    -e '/^HTTP_PROXY=/d' -e '/^HTTPS_PROXY=/d' -e '/^NO_PROXY=/d' /etc/environment

  # Add current proxy environment variables to /etc/environment
  if [[ -n "${HTTP_PROXY:-}" ]]; then echo "HTTP_PROXY=${HTTP_PROXY}" >> /etc/environment; fi
  if [[ -n "${http_proxy:-}" ]]; then echo "http_proxy=${http_proxy}" >> /etc/environment; fi
  if [[ -n "${HTTPS_PROXY:-}" ]]; then echo "HTTPS_PROXY=${HTTPS_PROXY}" >> /etc/environment; fi
  if [[ -n "${https_proxy:-}" ]]; then echo "https_proxy=${https_proxy}" >> /etc/environment; fi
  if [[ -n "${NO_PROXY:-}" ]]; then echo "NO_PROXY=${NO_PROXY}" >> /etc/environment; fi
  if [[ -n "${NO_PROXY:-}" ]]; then echo "no_proxy=${no_proxy}" >> /etc/environment; fi

  # Persist for all shell sessions
  local profile_script="/etc/profile.d/proxy.sh"
  echo "# Proxy settings from Dataproc init action" > "${profile_script}"
  if [[ -n "${HTTP_PROXY:-}" ]]; then echo "export HTTP_PROXY='${HTTP_PROXY}'" >> "${profile_script}"; fi
  if [[ -n "${http_proxy:-}" ]]; then echo "export http_proxy='${http_proxy}'" >> "${profile_script}"; fi
  if [[ -n "${HTTPS_PROXY:-}" ]]; then echo "export HTTPS_PROXY='${HTTPS_PROXY}'" >> "${profile_script}"; fi
  if [[ -n "${https_proxy:-}" ]]; then echo "export https_proxy='${https_proxy}'" >> "${profile_script}"; fi
  if [[ -n "${NO_PROXY:-}" ]]; then echo "export NO_PROXY='${NO_PROXY}'" >> "${profile_script}"; fi
  if [[ -n "${no_proxy:-}" ]]; then echo "export no_proxy='${no_proxy}'" >> "${profile_script}"; fi

  # Source the script to apply settings to the current shell
  source "${profile_script}"

  # Configure gcloud proxy
  local gcloud_version
  local -r min_gcloud_proxy_ver="547.0.0"
  gcloud_version=$(gcloud version --format="value(google_cloud_sdk)" 2>/dev/null || echo "0.0.0")
  if version_ge "${gcloud_version}" "${min_gcloud_proxy_ver}"; then
    if [[ -n "${http_proxy_val}" ]]; then
      local proxy_host=$(echo "${http_proxy_val}" | cut -d: -f1)
      local proxy_port=$(echo "${http_proxy_val}" | cut -d: -f2)
      gcloud config set proxy/type http
      gcloud config set proxy/address "${proxy_host}"
      gcloud config set proxy/port "${proxy_port}"
    else
      gcloud config unset proxy/type
      gcloud config unset proxy/address
      gcloud config unset proxy/port
    fi
  fi

  # Install the HTTPS proxy's certificate
  local proxy_ca_pem=""
  local trusted_pem_path=""
  METADATA_HTTP_PROXY_PEM_URI="$(get_metadata_attribute http-proxy-pem-uri '')"
  if [[ -n "${METADATA_HTTP_PROXY_PEM_URI}" ]] ; then
    if [[ ! "${METADATA_HTTP_PROXY_PEM_URI}" =~ ^gs:// ]] ; then echo "ERROR: http-proxy-pem-uri value must start with gs://" ; exit 1 ; fi
    echo "DEBUG: set_proxy: Processing http-proxy-pem-uri='${METADATA_HTTP_PROXY_PEM_URI}'"
    local trusted_pem_dir
    if is_debuntu ; then
      trusted_pem_dir="/usr/local/share/ca-certificates"
      proxy_ca_pem="${trusted_pem_dir}/proxy_ca.crt"
      mkdir -p "${trusted_pem_dir}"
      gsutil cp "${METADATA_HTTP_PROXY_PEM_URI}" "${proxy_ca_pem}" || { echo "ERROR: Failed to download proxy CA cert from GCS." ; exit 1 ; }
      update-ca-certificates
      trusted_pem_path="/etc/ssl/certs/ca-certificates.crt"
    elif is_rocky ; then
      trusted_pem_dir="/etc/pki/ca-trust/source/anchors"
      proxy_ca_pem="${trusted_pem_dir}/proxy_ca.crt"
      mkdir -p "${trusted_pem_dir}"
      gsutil cp "${METADATA_HTTP_PROXY_PEM_URI}" "${proxy_ca_pem}" || { echo "ERROR: Failed to download proxy CA cert from GCS." ; exit 1 ; }
      update-ca-trust
      trusted_pem_path="/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem"
    fi
    export REQUESTS_CA_BUNDLE="${trusted_pem_path}"
    echo "DEBUG: set_proxy: trusted_pem_path set to '${trusted_pem_path}'"

    # Add to Java/Conda trust stores
    if [[ -f "/etc/environment" ]]; then
        JAVA_HOME="$(awk -F= '/^JAVA_HOME=/ {print $2}' /etc/environment)"
        if [[ -n "${JAVA_HOME:-}" && -f "${JAVA_HOME}/bin/keytool" ]]; then
            "${JAVA_HOME}/bin/keytool" -import -cacerts -storepass changeit -noprompt -alias swp_ca -file "${proxy_ca_pem}"
        fi
    fi
    if command -v conda &> /dev/null ; then
      local conda_cert_file="/opt/conda/default/ssl/cacert.pem"
      if [[ -f "${conda_cert_file}" ]]; then
        local ca_subject=$(openssl crl2pkcs7 -nocrl -certfile "${proxy_ca_pem}" | openssl pkcs7 -print_certs -noout | grep ^subject)
        openssl crl2pkcs7 -nocrl -certfile "${conda_cert_file}" | openssl pkcs7 -print_certs -noout | grep -Fxq "${ca_subject}" || {
          cat "${proxy_ca_pem}" >> "${conda_cert_file}"
        }
      fi
    fi
  fi

  if [[ -n "${http_proxy_val}" ]]; then

    local proxy_host=$(echo "${http_proxy_val}" | cut -d: -f1)
    local proxy_port=$(echo "${http_proxy_val}" | cut -d: -f2)

    echo "DEBUG: set_proxy: Testing TCP connection to proxy ${proxy_host}:${proxy_port}..."
    if ! nc -zv -w 5 "${proxy_host}" "${proxy_port}"; then
      echo "ERROR: Failed to establish TCP connection to proxy ${proxy_host}:${proxy_port}."
      exit 1
    fi

    echo "DEBUG: set_proxy: Testing external site access via proxy..."
    local test_url="https://www.google.com"
    local curl_test_args=()
    if [[ -n "${trusted_pem_path}" ]]; then
      curl_test_args+=(--cacert "${trusted_pem_path}")
    fi
    if curl "${curl_test_args[@]}" -vL --retry 3 --retry-delay 5 -o /dev/null "${test_url}"; then
      echo "DEBUG: set_proxy: Successfully fetched ${test_url} via proxy."
    else
      echo "ERROR: Failed to fetch ${test_url} via proxy ${HTTP_PROXY}."
      exit 1
    fi
  fi

  # Configure package managers
  local pkg_proxy_conf_file
  local effective_proxy="${http_proxy_val:-${https_proxy_val}}"
  if [[ -z "${effective_proxy}" ]]; then
      echo "DEBUG: set_proxy: No HTTP or HTTPS proxy set for package managers."
  elif is_debuntu ; then
    pkg_proxy_conf_file="/etc/apt/apt.conf.d/99proxy"
    echo "Acquire::http::Proxy \"http://${effective_proxy}\";" > "${pkg_proxy_conf_file}"
    echo "Acquire::https::Proxy \"http://${effective_proxy}\";" >> "${pkg_proxy_conf_file}"
  elif is_rocky ; then
    pkg_proxy_conf_file="/etc/dnf/dnf.conf"
    touch "${pkg_proxy_conf_file}"
    sed -i.bak '/^proxy=/d' "${pkg_proxy_conf_file}"
    if grep -q "^\[main\]" "${pkg_proxy_conf_file}"; then
      sed -i.bak "/^\\\[main\\\\]/a proxy=http://${effective_proxy}" "${pkg_proxy_conf_file}"
    else
      echo -e "[main]\nproxy=http://${effective_proxy}" >> "${pkg_proxy_conf_file}"
    fi
  fi

  # Configure dirmngr
  if is_debuntu ; then
    if ! dpkg -l | grep -q dirmngr; then
      execute_with_retries apt-get install -y -qq dirmngr
    fi
  elif is_rocky ; then
    if ! rpm -q gnupg2-smime; then
      execute_with_retries dnf install -y -q gnupg2-smime
    fi
  fi
  mkdir -p /etc/gnupg
  local dirmngr_conf="/etc/gnupg/dirmngr.conf"
  touch "${dirmngr_conf}"
  sed -i.bak '/^http-proxy/d' "${dirmngr_conf}"
  if [[ -n "${HTTP_PROXY:-}" ]]; then
    echo "http-proxy ${HTTP_PROXY}" >> "${dirmngr_conf}"
  fi
}

function repair_boto() {
  local boto_file="/etc/boto.cfg"
  if [[ -f "${boto_file}" ]]; then
    echo "DEBUG: repair_boto: Repairing and deduplicating ${boto_file}" >&2
    
    # 1. Deduplicate sections (fix for DuplicateSectionError)
    # Use a more robust perl one-liner that also handles the content within duplicate sections
    # by only keeping the first occurrence of each section and its variables.
    # Comments and blank lines are always printed to preserve structure.
    perl -i -ne '
      print and next if /^\s*(?:[#;]|$)/;
      if (/^\[([^\]]+)\]/) {
        $skip = $seen{$1}++;
      }
      print unless $skip;
    ' "${boto_file}"
    
    # 2. Fix universe_domain if it is still a variable
    local universe_domain
    universe_domain=$(get_metadata_attribute 'universe-domain' 'googleapis.com')
    # Use a more robust replacement that handles potential escaping issues
    UNIVERSE_DOMAIN="${universe_domain}" perl -i -pe 's/\$\{universe_domain\}/$ENV{UNIVERSE_DOMAIN}/g' "${boto_file}"
    # Also fix cases where it might have been partially expanded to storage.$
    UNIVERSE_DOMAIN="${universe_domain}" perl -i -pe 's/storage\.\$/storage.$ENV{UNIVERSE_DOMAIN}/g' "${boto_file}"

    # 3. Apply proxy if set
    local meta_http_proxy=$(get_metadata_attribute 'http-proxy' '')
    local meta_proxy_uri=$(get_metadata_attribute 'proxy-uri' '')
    local effective_proxy="${meta_http_proxy:-${meta_proxy_uri}}"
    
    if [[ -n "${effective_proxy}" ]]; then
      local proxy_host="${effective_proxy%:*}"
      local proxy_port="${effective_proxy##*:}"
      
      sed -i -e '/^proxy =/d' -e '/^proxy_port =/d' "${boto_file}"
      if grep -q "^\[Boto\]" "${boto_file}"; then
        sed -i "/^\[Boto\]/a proxy = ${proxy_host}\nproxy_port = ${proxy_port}" "${boto_file}"
      else
        echo -e "\n[Boto]\nproxy = ${proxy_host}\nproxy_port = ${proxy_port}" >> "${boto_file}"
      fi
    fi
    echo "DEBUG: repair_boto: Updated ${boto_file}" >&2
  fi
}

# --- Execution ---
set_proxy
repair_boto
echo "DEBUG: gce-proxy-setup.sh complete." >&2
