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
# However, Cuda 12.1.1 - Driver v530.30.02 is used for Ubuntu 18 only
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
# --- Package Management ---

function execute_with_retries() {
  local -r cmd="$1"
  local -r max_retries=${2:-5}
  local -r delay=${3:-5}
  for ((i = 0; i < max_retries; i++)); do
    if eval "${cmd}"; then
      return 0
    fi
    echo "WARN: Command failed: [${cmd}]. Retrying in ${delay} seconds..." >&2
    sleep "${delay}"
  done
  echo "ERROR: Command failed after ${max_retries} retries: [${cmd}]" >&2
  return 1
}

function ensure_required_packages() {
  export DEBIAN_FRONTEND=noninteractive
  local os_id=$(grep '^ID=' /etc/os-release | cut -d= -f2 | xargs)
  local packages_to_install=()

  # Packages needed by the evaluation script itself
  if ! command -v xmlstarlet &>/dev/null; then packages_to_install+=("xmlstarlet"); fi
  if ! command -v jq &>/dev/null; then packages_to_install+=("jq"); fi
  if ! command -v git &>/dev/null; then packages_to_install+=("git"); fi
  if ! python3 -m venv --help &>/dev/null; then packages_to_install+=("python3-venv"); fi

  # Packages for NVIDIA driver installation (Debian/Ubuntu only)
  if [[ "${os_id}" == "debian" || "${os_id}" == "ubuntu" ]]; then
    packages_to_install+=("dkms" "linux-headers-$(uname -r)")
  fi

  if [[ ${#packages_to_install[@]} -eq 0 ]]; then
    echo "INFO: All required packages are already installed." >&2
    return 0
  fi

  echo "INFO: Installing missing packages: ${packages_to_install[@]}" >&2

  if [[ "${os_id}" == "debian" || "${os_id}" == "ubuntu" ]]; then
    execute_with_retries "apt-get -o DPkg::Lock::Timeout=60 update -qq" 3 2
    if ! execute_with_retries "apt-get -o DPkg::Lock::Timeout=60 install -y -qq ${packages_to_install[*]}"; then
      echo "ERROR: Failed to install required packages." >&2
      return 1
    fi
  elif [[ "${os_id}" == "rocky" ]]; then
    # DNF handles locks more gracefully, timeout option not needed.
    if ! execute_with_retries "dnf install -y -q ${packages_to_install[*]}"; then
      echo "ERROR: Failed to install required packages." >&2
      return 1
    fi
  else
    echo "WARN: Unsupported OS for package installation: ${os_id}" >&2
    return 1
  fi
  echo "INFO: Required packages installed successfully." >&2
  return 0
}
#!/bin/bash

# tmp/spark-rapids.sh_function-evaluate_gpu_setup
# Functions to detect GPU hardware and software components

# --- Variables ---
NVIDIA_SMI_XML_FILE="/tmp/nvidia-smi-dump.xml"
EVALUATION_OUTPUT_FILE="/tmp/gpu_evaluation.json"
PUBLIC_CERT_FILE="/tmp/mok_public.der"
MAMBA_SEARCH_CACHE="/tmp/mamba_search_output.txt"
KEY_PACKAGES=("pytorch" "tensorflow" "rapids" "xgboost" "cudatoolkit" "cudnn")
# --- System Helper functions ---

function ensure_xmlstarlet() {
  if ! command -v xmlstarlet &>/dev/null; then
    echo "ERROR: xmlstarlet not found. Please run ensure_required_packages first." >&2
    return 1
  fi
  return 0
}

function ensure_jq() {
  if ! command -v jq &>/dev/null; then
    echo "ERROR: jq not found. Please run ensure_required_packages first." >&2
    return 1
  fi
  return 0
}

function get_secure_boot_status() {
  if command -v mokutil &>/dev/null; then
    mokutil --sb-state 2>/dev/null | awk '{print $2}'
  else
    echo "unknown"
  fi
}

function _parse_proxy_url() {
  local url="$1"
  local protocol="$(echo "$url" | grep :// | sed -e's,===.*,,')"
  local host_port="$(echo "$url" | sed -e "s,$protocol//,,")"
  local host="$(echo "$host_port" | cut -d: -f1)"
  local port="$(echo "$host_port" | cut -d: -f2)"
  if [[ "$host" == "$port" ]] || [[ -z "$port" ]]; then
    if [[ "$protocol" == "http" ]]; then port="80"; fi
    if [[ "$protocol" == "https" ]]; then port="443"; fi
  fi
  echo "${protocol}://${host}:${port}"
}

function get_proxy_settings() {
  local settings=()
  if [[ -n "${HTTP_PROXY:-}" ]]; then settings+=("HTTP_PROXY=$(_parse_proxy_url "${HTTP_PROXY}")"); fi
  if [[ -n "${HTTPS_PROXY:-}" ]]; then settings+=("HTTPS_PROXY=$(_parse_proxy_url "${HTTPS_PROXY}")"); fi
  local meta_http_proxy=$(get_metadata_attribute http-proxy)
  local meta_https_proxy=$(get_metadata_attribute https-proxy)
  if [[ -n "${meta_http_proxy}" ]]; then settings+=("META_HTTP=$(_parse_proxy_url "${meta_http_proxy}")"); fi
  if [[ -n "${meta_https_proxy}" ]]; then settings+=("META_HTTPS=$(_parse_proxy_url "${meta_https_proxy}")"); fi
  if [[ ${#settings[@]} -eq 0 ]]; then echo "None"; else printf "%s; " "${settings[@]}"; fi
}

# --- MOK Helper functions ---
function get_cert_ski() {
  local cert_path="$1"
  if [[ ! -f "${cert_path}" ]]; then echo ""; return; fi
  openssl x509 -inform DER -in "${cert_path}" -text -noout 2>/dev/null | grep -A 1 "X509v3 Subject Key Identifier" | grep -v "X509v3 Subject Key Identifier" | tr -d '\s:' | tr 'A-Z' 'a-z'
}

function fetch_public_mok() {
  local PUBSN=$(get_metadata_attribute public_secret_name)
  if [[ -z "${PUBSN}" ]]; then echo "WARN: public_secret_name not set" >&2; return 1; fi
  local secret_project=$(get_metadata_attribute secret_project)

  local cmd="gcloud secrets versions access latest --secret=${PUBSN}"
  if [[ -n "${secret_project}" ]]; then
    cmd="${cmd} --project=${secret_project}"
  fi

  if ! command -v gcloud &>/dev/null; then echo "ERROR: gcloud not found" >&2; return 1; fi

  if ${cmd} | base64 --decode > "${PUBLIC_CERT_FILE}" 2>/dev/null; then
    if [[ -s "${PUBLIC_CERT_FILE}" ]]; then
      return 0
    else
      echo "ERROR: Fetched public secret is empty." >&2
      rm -f "${PUBLIC_CERT_FILE}"
      return 1
    fi
  else
    echo "ERROR: Failed to fetch public secret ${PUBSN}." >&2
    rm -f "${PUBLIC_CERT_FILE}"
    return 1
  fi
}

function get_mok_ski_from_file() {
  if [[ ! -f "${PUBLIC_CERT_FILE}" ]]; then echo ""; return; fi
  get_cert_ski "${PUBLIC_CERT_FILE}" | xargs
}

function get_trusted_skis() {
  dmesg | grep "Loaded X.509 cert" | awk -F': ' '{print $NF}' | tr -d "'" | sort -u
}

function is_signing_key_trusted() {
  local mok_ski=$(get_mok_ski_from_file)
  if [[ -z "${mok_ski}" ]]; then return 1; fi
  local trusted_skis=$(get_trusted_skis)
  if echo "${trusted_skis}" | grep -qi "${mok_ski}"; then
    return 0
  else
    return 1
  fi
}
# --- PCI Helper functions ---
# This function maps NVIDIA GPU device IDs to their model names.
# The vendor ID for NVIDIA is 10de.
# Device IDs can be cross-referenced with public PCI ID databases
# or the local pci.ids file (e.g., /usr/share/misc/pci.ids).
function pci_id_to_model() {
  local device_id="$1"
  case "${device_id^^}" in # Convert to uppercase for case-insensitivity
    "15F7") echo "Tesla P100" ;;
    "15F8") echo "Tesla P100" ;;
    "15F9") echo "Tesla P100" ;;
    "1B38") echo "Tesla P40" ;;
    "1BB3") echo "Tesla P4" ;;
    "1DB1") echo "Tesla V100" ;;
    "1DB2") echo "Tesla V100" ;;
    "1DB3") echo "Tesla V100" ;;
    "1DB4") echo "Tesla V100" ;;
    "1DB5") echo "Tesla V100 32GB" ;;
    "1DB6") echo "Tesla V100 32GB" ;;
    "1DB8") echo "Tesla V100 32GB" ;;
    "1EB8") echo "Tesla T4" ;;
    "20B0") echo "A100 40GB" ;;
    "20B1") echo "A100 40GB" ;;
    "20B2") echo "A100 80GB" ;;
    "20B5") echo "A100 80GB" ;;
    "2330") echo "H100 SXM5" ;;
    "2331") echo "H100 PCIe" ;;
    "27B8") echo "L4" ;;
    "2901") echo "B200" ;;
    "2920") echo "B100" ;;
    "29BC") echo "B100" ;;
    *) echo "" ;;
  esac
}

function shorten_gpu_model() {
  local full_model="$1"
  if [[ "${full_model}" == *"["* ]]; then
    echo "${full_model}" | perl -ne 'print $1 if /.*\s+\[(.*?)\]/' || echo "${full_model}"
  else
    echo "${full_model}" | sed 's/.*: //'
  fi
}

function summarize_gpu_models() {
  local models_str="$1"
  if [[ -z "${models_str}" ]]; then
    echo "Unknown"
    return
  fi

  local short_models_array=()
  while read -r line; do
    short_models_array+=("$(shorten_gpu_model "${line}")")
  done <<< "${models_str}"

  local unique_models=$(printf "%s
" "${short_models_array[@]}" | sort | uniq -c)
  local summary=()
  while read -r line; do
    local count=$(echo "${line}" | awk '{print $1}')
    local model=$(echo "${line}" | awk '{$1=""; print $0}' | sed 's/^[ ]*//')
    if [[ "${count}" -gt 1 ]]; then
      summary+=("${model} (x${count})")
    else
      summary+=("${model}")
    fi
  done <<< "${unique_models}"

  echo "$(printf "%s, " "${summary[@]}" | sed 's/, $//')"
}

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

function get_sysfs_gpu_devices() {
  local devices=()
  for dev in /sys/bus/pci/devices/*; do
    if [[ -f "${dev}/vendor" ]] && [[ "$(cat "${dev}/vendor")" == "0x10de" ]]; then
      if [[ -f "${dev}/class" ]]; then
        local class_code=$(cat "${dev}/class")
        if [[ "${class_code:0:4}" == "0x03" ]]; then
          local device_id=$(cat "${dev}/device" | sed 's/0x//')
          local model=$(pci_id_to_model "${device_id}")
          if [[ -z "${model}" ]]; then
            model="Unknown NVIDIA Device (10de:${device_id})"
          fi
          devices+=("${model}")
        fi
      fi
    fi
  done
  printf "%s
" "${devices[@]}"
}
# --- NVIDIA SMI Helper functions ---
function dump_nvidia_smi_xml() {
  if [[ -f "${NVIDIA_SMI_XML_FILE}" ]]; then
    return 0
  fi
  if command -v nvidia-smi &>/dev/null; then
    local tmp_xml_file=$(mktemp)
    local stderr_file=$(mktemp)
    if ! nvidia-smi -q -x > "${tmp_xml_file}" 2> "${stderr_file}"; then
      echo "ERROR: nvidia-smi -q -x command failed. Exit code: $?" >&2
      cat "${stderr_file}" >&2
      rm -f "${tmp_xml_file}" "${stderr_file}"
      return 1
    fi
    rm -f "${stderr_file}"
    if [[ -s "${tmp_xml_file}" ]]; then
      mv "${tmp_xml_file}" "${NVIDIA_SMI_XML_FILE}"
      return 0
    else
      echo "ERROR: nvidia-smi -q -x produced an empty file." >&2
      rm -f "${tmp_xml_file}"
      return 1
    fi
  else
    echo "ERROR: nvidia-smi command not found." >&2
    return 1
  fi
}

function query_nvidia_xml() {
  local xpath="$1"
  if [[ -f "${NVIDIA_SMI_XML_FILE}" ]] && ensure_xmlstarlet; then
    xmlstarlet sel -t -v "${xpath}" "${NVIDIA_SMI_XML_FILE}" 2>/dev/null || echo ""
  else
    echo ""
  fi
}

function get_driver_version_from_xml() {
  query_nvidia_xml "/nvidia_smi_log/driver_version"
}

function get_gpu_count_from_xml() {
  query_nvidia_xml "count(/nvidia_smi_log/gpu)"
}

function get_gpu_models_from_xml() {
  if [[ -f "${NVIDIA_SMI_XML_FILE}" ]] && ensure_xmlstarlet; then
    xmlstarlet sel -t -v "/nvidia_smi_log/gpu/product_name" "${NVIDIA_SMI_XML_FILE}" 2>/dev/null
  else
    echo ""
  fi
}

# --- Driver/CUDA Helper functions ---
function detect_nvidia_driver() {
  if ! command -v modprobe &>/dev/null; then return 1; fi
  if ! command -v modinfo &>/dev/null; then return 1; fi

  if modprobe -n -q nvidia; then # Check if module exists
    if ! lsmod | grep -q "^nvidia"; then # If not loaded, try to load it
      if ! modprobe nvidia; then return 1; fi
    fi
    if [[ -c /dev/nvidia0 ]]; then return 0; fi
  fi
  return 1
}

function get_nvidia_module_signature_status() {
  local sb_status=$(get_secure_boot_status)
  if [[ "${sb_status}" == "enabled" ]]; then
    local PSN=$(get_metadata_attribute private_secret_name)
    if [[ -n "${PSN}" ]]; then
      if modinfo nvidia | grep -qi "^signer:"; then
        echo "signed"
      else
        echo "unsigned"
      fi
    else
      echo "unknown_no_psn"
    fi
  else
    echo "unsigned"
  fi
}

function detect_cuda_toolkit() {
  export PATH=/usr/local/cuda/bin:${PATH}
  if command -v nvcc &>/dev/null; then return 0; fi
  return 1
}

function get_cuda_version() {
  if command -v nvcc &>/dev/null; then
    nvcc --version 2>/dev/null | grep "release" | sed -n 's/.*release \([0-9.]*\).*/\1/p' || echo ""
  else
    echo ""
  fi
}
# --- Spark JAR Helper functions ---
function find_gpu_spark_jars() {
  local common_paths=("/usr/lib/spark/jars/" "/opt/spark/jars/" "/usr/local/share/google/dataproc/lib/")
  local jq_expr="."
  local jar_found=false
  local output_file="$(mktemp /tmp/find_gpu_spark_jars.XXXXXX.json)"

  local jar_patterns=(
    "rapids-spark:rapids-spark_2.12-([0-9.]+|[a-zA-Z0-9.-]+).jar"
    "cudf:cudf-([0-9.]+|[a-zA-Z0-9.-]+).jar"
    "xgboost4j-spark-gpu:xgboost4j-spark-gpu_2.12-([0-9.]+|[a-zA-Z0-9.-]+).jar"
    "xgboost4j-gpu:xgboost4j-gpu_2.12-([0-9.]+|[a-zA-Z0-9.-]+).jar"
  )

  for path in "${common_paths[@]}"; do
    if [[ -d "${path}" ]]; then
      for pattern_item in "${jar_patterns[@]}"; do
        local jar_name="${pattern_item%%:*}"
        local pattern="${pattern_item#*:}"
        while IFS= read -r jar_path; do
          if [[ -z "${jar_path}" ]]; then continue; fi
          local filename=$(basename "${jar_path}")
          if [[ "${filename}" =~ ${pattern} ]]; then
            local version="${BASH_REMATCH[1]}"
            jq_expr="${jq_expr} | .\"${jar_name}\" = \"${version}\""
            jar_found=true
            break # Found this jar type in this path, move to next type
          fi
        done < <(find "${path}" -type f -name "${jar_name}*" 2>/dev/null)
      done
    fi
  done

  if [[ "${jar_found}" == "true" ]]; then
    echo "{}" | jq -c "${jq_expr}" > "${output_file}"
  else
    echo "{}" > "${output_file}"
  fi
  echo "${output_file}"
}
# --- Conda/Mamba Helpers ---
function get_conda_base() {
  local dataproc_image_version
  dataproc_image_version=$(get_metadata_attribute dataproc-image-version "")
  if [[ -z "${dataproc_image_version}" ]]; then
    # Fallback if metadata is not available
    if [[ -d "/opt/conda/bin" ]]; then echo "/opt/conda"; return; fi
    if [[ -d "/opt/conda/miniconda3/bin" ]]; then echo "/opt/conda/miniconda3"; return; fi
    echo ""
    return
  fi

  if dpkg --compare-versions "${dataproc_image_version}" lt "2.3"; then
    echo "/opt/conda/miniconda3"
  else
    echo "/opt/conda"
  fi
}

function run_mamba_search() {
  local force_refresh=false
  if [[ "$1" == "--force" ]]; then
    force_refresh=true
  fi

  if [[ -f "${MAMBA_SEARCH_CACHE}" && "${force_refresh}" == "false" ]]; then
    # Check if the cache is older than 10 hours (36000 seconds)
    local cache_age=$(($(date +%s) - $(stat -c %Y "${MAMBA_SEARCH_CACHE}")))
    if [[ "${cache_age}" -lt 36000 ]]; then
      echo "INFO: Using cached mamba search results from ${MAMBA_SEARCH_CACHE}" >&2
      return 0
    else
      echo "INFO: Mamba search cache is older than 10 hours, refreshing..." >&2
    fi
  fi

  echo "INFO: Running mamba search (this may take a moment)..." >&2
  local conda_base=$(get_conda_base)
  if [[ -z "${conda_base}" ]]; then
    echo "ERROR: Conda base not found." >&2
    return 1
  fi
  local mamba_path="${conda_base}/bin/mamba"
  if [[ ! -x "${mamba_path}" ]]; then
    mamba_path="${conda_base}/bin/conda"
    if [[ ! -x "${mamba_path}" ]]; then
      echo "ERROR: mamba or conda executable not found in ${conda_base}/bin" >&2
      return 1
    fi
  fi

  local search_terms=()
  for pkg in "${KEY_PACKAGES[@]}"; do
    search_terms+=("*${pkg}*")
  done

  local mamba_log="/tmp/mamba_search.log"
  rm -f "${mamba_log}"
  if ! "${mamba_path}" search -c conda-forge -c nvidia -c rapidsai "${search_terms[@]}" > "${MAMBA_SEARCH_CACHE}" 2> "${mamba_log}"; then
    echo "ERROR: mamba search failed. Check ${mamba_log}" >&2
    # cat "${mamba_log}" >&2 # Optionally cat the log on error
    return 1
  fi
  rm -f "${mamba_log}"
}

function parse_mamba_search() {
  if [[ ! -f "${MAMBA_SEARCH_CACHE}" ]]; then
    echo "{}"
    return
  fi

  declare -A pkg_versions
  local pkg_list
  pkg_list=$(printf "%s " "${KEY_PACKAGES[@]}")

  local awk_output="/tmp/awk_mamba_out.txt"
  rm -f "${awk_output}"

  # AWK script to extract package and version
  awk -v key_packages="${pkg_list}" '
  BEGIN {
    split(key_packages, keys, " ");
    for (i in keys) {
      if (keys[i] != "") key_map[keys[i]] = 1;
    }
    current_pkg = "";
    collecting = 0;
  }
  /^[a-zA-Z0-9._-]+ /{
    pkg_name = $1;
    current_pkg = ""; # Reset current_pkg
    for (key in key_map) {
      if (index(pkg_name, key) > 0) {
        current_pkg = key; # Assign the KEY_PACKAGES key
        break;
      }
    }
    collecting = 0;
  }
  /Other Versions \([0-9]+\):/ {
    if (current_pkg != "") {
      collecting = 1;
    }
    getline; getline; next;
  }
  collecting && /^[ ]+[0-9]+\.[0-9]+.*$/ {
    if (current_pkg != "") {
      print current_pkg "~" $1;
    }
  }
  /^$/ { collecting = 0 }
  ' "${MAMBA_SEARCH_CACHE}" > "${awk_output}"

  while IFS='~' read -r pkg version; do
    if [[ -n "${pkg}" ]]; then
      # Append version if not already present
      if [[ ! " ${pkg_versions[${pkg}]} " =~ " ${version} " ]]; then
        pkg_versions[${pkg}]+="${version} "
      fi
    fi
  done < "${awk_output}"
  rm -f "${awk_output}"

  # Build JSON with jq
  local jq_expr="."
  for pkg in "${!pkg_versions[@]}"; do
    local versions_str="${pkg_versions[${pkg}]}"
    # Create a JSON array string
    local versions_json=$(echo "${versions_str}" | xargs -n1 | sort -uV | jq -Rsc 'split("\n") | map(select(length > 0))')
    if [[ "${versions_json}" != "[]" ]] && [[ -n "${versions_json}" ]]; then
      jq_expr="${jq_expr} | .[\"${pkg}\"] = ${versions_json}"
    fi
  done

  if [[ "${jq_expr}" == "." ]]; then
    echo "{}"
  else
    echo "{}" | jq -c "${jq_expr}"
  fi
}
# --- Package Detail Helpers ---
function get_package_details() {
  local installed=$1
  local version=$2
  local available_list_json=$3

  jq -n --argjson installed "${installed}" --arg installed_version "${version}" --argjson available_versions "${available_list_json}" '{ installed: $installed, installed_version: (if $installed then $installed_version else null end), available_versions: $available_versions }'
}

function get_apt_available_versions() {
  local pkg_pattern="$1"
  local all_versions=()
  local pkg_names=()
  local search_term
  local grep_pattern

  if [[ "${pkg_pattern}" == "libcudnn*-dev" ]]; then
    search_term="libcudnn"
    grep_pattern='libcudnn[0-9]*-dev'
  elif [[ "${pkg_pattern}" == "cuda-toolkit-*-*" ]]; then
    search_term="cuda-toolkit"
    grep_pattern='cuda-toolkit-'
  else
    search_term="${pkg_pattern}"
    grep_pattern="${pkg_pattern}"
  fi

  while read -r line; do
    local name=$(echo "${line}" | awk '{print $1}')
    pkg_names+=("${name}")
  done < <(apt-cache search "${search_term}" 2>/dev/null | grep -E "${grep_pattern}")

  if [[ ${#pkg_names[@]} -eq 0 ]]; then
    echo "[]"
    return
  fi

  for pkg_name in "${pkg_names[@]}"; do
    policy_output=$(apt-cache policy "${pkg_name}" 2>/dev/null)
    while read -r version; do
      all_versions+=("${version}")
    done < <(echo "${policy_output}" | awk '/  [0-9]/{print $1}')
  done

  printf "%s\n" "${all_versions[@]}" | sort -uV | jq -Rsc 'split("\n") | map(select(length > 0))' || echo "[]"
}

function get_dnf_available_versions() {
  local pkg_name="$1"
  dnf repoquery --showversions --showduplicates "${pkg_name}" 2>/dev/null | awk -F':' '{print $NF}' | sed -r 's/^[^-]+-//; s/\.(x86_64|noarch|aarch64)//' | sort -uV | jq -Rsc 'split("\n") | map(select(length > 0))' || echo "[]"
}

function get_system_available_versions() {
  local pkg_name="$1"
  if is_ubuntu || is_debian; then
    get_apt_available_versions "${pkg_name}"
  elif is_rocky; then
    get_dnf_available_versions "${pkg_name}"
  else
    echo "[]"
  fi
}

function get_system_package_details() {
  local pkg_name_pattern="$1"
  local version="Not Found"
  local installed="false"

  if command -v dpkg &>/dev/null; then
    # Check for installed packages matching the pattern
    local installed_pkgs=$(dpkg -l "${pkg_name_pattern}" 2>/dev/null | grep "^ii" | awk '{print $2}' | head -n 1)
    if [[ -n "${installed_pkgs}" ]]; then
      installed="true"
      # Get version of the first matched installed package
      version=$(dpkg -l "${installed_pkgs}" 2>/dev/null | grep "^ii" | awk '{print $3}')
    fi
  elif command -v rpm &>/dev/null; then
    if rpm -q "${pkg_name_pattern}" >/dev/null 2>&1; then
      installed="true"
      version=$(rpm -q --queryformat '%{VERSION}-%{RELEASE}' "${pkg_name_pattern}" | head -n 1)
    fi
  fi
  local available_versions=$(get_system_available_versions "${pkg_name_pattern}")
  get_package_details "${installed}" "${version}" "${available_versions}"
}
# --- Hadoop/Spark GPU Configuration Checks ---

HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
SPARK_CONF_DIR=${SPARK_CONF_DIR:-/etc/spark/conf}

function get_xml_property() {
    local file="$1"
    local property="$2"
    local result

    if [[ ! -f "${file}" ]]; then
        echo "Not Found"
        return
    fi

    # This is a bit complex to do with shell tools alone.
    # We'll use python if available, otherwise grep as a fallback.
    if command -v python >/dev/null; then
        result=$(python -c "import xml.etree.ElementTree as ET; tree = ET.parse('${file}'); root = tree.getroot(); val = root.find('.//property[name='${property}']/value'); print(val.text if val is not None else 'Not Found')" 2>/dev/null)
    else
        # Fallback to grep, less reliable for XML structure
        result=$(grep -A 1 "${property}" "${file}" | grep '<value>' | sed -e 's/<[^>]*>//g' || echo "Not Found")
    fi
    echo "${result}"
}

function check_yarn_gpu_config() {
    local yarn_site="${HADOOP_CONF_DIR}/yarn-site.xml"
    local capacity_scheduler="${HADOOP_CONF_DIR}/capacity-scheduler.xml"
    local resource_types="${HADOOP_CONF_DIR}/resource-types.xml"
    local config_status={}

    config_status=$(echo "${config_status}" | jq --arg val "$(get_xml_property "${yarn_site}" "yarn.nodemanager.resource-plugins")" '.["yarn.nodemanager.resource-plugins"] = $val')
    config_status=$(echo "${config_status}" | jq --arg val "$(get_xml_property "${yarn_site}" "yarn.resource-types")" '.["yarn.resource-types"] = $val')
    config_status=$(echo "${config_status}" | jq --arg val "$(get_xml_property "${capacity_scheduler}" "yarn.scheduler.capacity.resource-calculator")" '.["yarn.scheduler.capacity.resource-calculator"] = $val')
    config_status=$(echo "${config_status}" | jq --arg val "$(get_xml_property "${resource_types}" "yarn.resource-types")" '.["resource-types.xml:yarn.resource-types"] = $val')

    echo "${config_status}" | jq -c .
}

function check_spark_gpu_config() {
    local spark_defaults="${SPARK_CONF_DIR}/spark-defaults.conf"
    local config_status={}

    if [[ ! -f "${spark_defaults}" ]]; then
        echo '{"spark-defaults.conf": "Not Found"}' | jq -c .
        return
    fi

    local plugins=$(grep "^spark.plugins" "${spark_defaults}" | awk -F'=' '{print $2}' | xargs || echo "Not Found")
    local executor_resources=$(grep "^spark.executor.resource.gpu.amount" "${spark_defaults}" | awk -F'=' '{print $2}' | xargs || echo "Not Found")
    local discovery_script=$(grep "^spark.executor.resource.gpu.discoveryScript" "${spark_defaults}" | awk -F'=' '{print $2}' | xargs || echo "Not Found")

    config_status=$(echo "${config_status}" | jq --arg val "${plugins}" '.["spark.plugins"] = $val')
    config_status=$(echo "${config_status}" | jq --arg val "${executor_resources}" '.["spark.executor.resource.gpu.amount"] = $val')
    config_status=$(echo "${config_status}" | jq --arg val "${discovery_script}" '.["spark.executor.resource.gpu.discoveryScript"] = $val')

    echo "${config_status}" | jq -c .
}

function check_gpu_discovery_script() {
    local script_path="/usr/lib/spark/scripts/gpu/getGpusResources.sh"
    if [[ -f "${script_path}" ]]; then
        if [[ -x "${script_path}" ]]; then
            echo '{"status": "Exists and Executable"}' | jq -c .
        else
            echo '{"status": "Exists but Not Executable"}' | jq -c .
        fi
    else
        echo '{"status": "Not Found"}' | jq -c .
    fi
}

function check_hadoop_configs() {
    local config_json="{}"
    config_json=$(echo "${config_json}" | jq --argjson val "$(check_yarn_gpu_config)" '.yarn = $val')
    config_json=$(echo "${config_json}" | jq --argjson val "$(check_spark_gpu_config)" '.spark = $val')
    config_json=$(echo "${config_json}" | jq --argjson val "$(check_gpu_discovery_script)" '.discovery_script = $val')
    local output_file="$(mktemp /tmp/check_hadoop_configs.XXXXXX.json)"
    echo "${config_json}" | jq -c . > "${output_file}"
    echo "${output_file}"
}
# --- System Checks ---
function check_system_gpu_packages() {
  local pkgs_json="{}"
  declare -A key_packages_map
  if is_rocky;
  then
    key_packages_map=(
      ["pytorch"]="python3-pytorch"
      ["tensorflow"]="python3-tensorflow"
      ["rapids"]="python3-rapids"
      ["xgboost"]="python3-xgboost"
      ["cudatoolkit"]="cuda-toolkit"
      ["cudnn"]="libcudnn*-devel"
    )
  else # Debian/Ubuntu
    key_packages_map=(
      ["pytorch"]="python3-pytorch"
      ["tensorflow"]="python3-tensorflow"
      ["rapids"]="python3-rapids"
      ["xgboost"]="python3-xgboost"
      ["cudatoolkit"]="cuda-toolkit-*-*"
      ["cudnn"]="libcudnn*-dev"
    )
  fi

  for key in "${!key_packages_map[@]}"; do
    local pkg_pattern="${key_packages_map[${key}]}"
    local pkg_details
    if [[ "${key}" == "rapids" ]]; then
      pkg_details='{"installed": false, "available_versions": [], "notes": "N/A - RAPIDS is not available as a system package"}'
    elif [[ "${key}" == "tensorflow" ]]; then
      pkg_details='{"installed": false, "available_versions": [], "notes": "N/A - TensorFlow is not typically installed as a system package"}'
    elif [[ "${key}" == "pytorch" ]]; then
      pkg_details='{"installed": false, "available_versions": [], "notes": "N/A - PyTorch is not typically installed as a system package"}'
    else
      pkg_details=$(get_system_package_details "${pkg_pattern}")
    fi
    pkgs_json=$(echo "${pkgs_json}" | jq --arg key "${key}" --argjson val "${pkg_details}" '.[$key] = $val')
  done
  echo "${pkgs_json}"
}

function check_system_cuda_repo() {
  local repo_file="Not Found"
  local keyring_file="Not Found"

  if is_ubuntu || is_debian; then
    repo_file=$(find /etc/apt/sources.list.d/ -name "cuda-*.list" -print -quit 2>/dev/null || echo "Not Found")
    keyring_file=$(find /usr/share/keyrings/ -name "cuda-archive-keyring.gpg" -print -quit 2>/dev/null || echo "Not Found")
  elif is_rocky; then
    repo_file=$(find /etc/yum.repos.d/ -name "cuda-*.repo" -print -quit 2>/dev/null || echo "Not Found")
    if rpm -q gpg-pubkey --qf "%{SUMMARY}
" | grep -qi CUDA; then
      keyring_file="Installed"
    else
      keyring_file="Not Found"
    fi
  fi

  jq -n --arg cuda_repo_file "${repo_file}" --arg cuda_keyring_file "${keyring_file}" '{cuda_repo_file: $cuda_repo_file, cuda_keyring_file: $cuda_keyring_file}'
}
# --- AIML Environment Detection ---
function _check_conda_env_details() {
  local conda_env="$1"
  local available_map_json="$2"
  local env_details="{}"
  local conda_base=$(get_conda_base)
  local conda_meta_dir="${conda_base}/conda-meta"
  if [[ "${conda_env}" != "base" ]]; then
    conda_meta_dir="${conda_base}/envs/${conda_env}/conda-meta"
  fi

  if [[ ! -d "${conda_meta_dir}" ]]; then echo "{}"; return 1; fi

  local gpu_pkgs_json="{}"
  for pkg in "${KEY_PACKAGES[@]}"; do
    local pkg_file=$(find "${conda_meta_dir}" -name "${pkg}-*.json" -print -quit 2>/dev/null)
    local version="Not Found"
    local installed="false"
    if [[ -n "${pkg_file}" ]]; then
      local filename=$(basename "${pkg_file}")
      version=$(echo "${filename}" | sed -n -e "s/${pkg}-\(.\+\)-\([^ ]*\)\.json/\1/p")
      if [[ -n "${version}" ]]; then installed="true"; else version="Unknown"; fi
    fi
    local available_list=$(echo "${available_map_json}" | jq -r --arg pkg "${pkg}" '.[$pkg] // []')
    local pkg_details=$(get_package_details "${installed}" "${version}" "${available_list}")
    gpu_pkgs_json=$(echo "${gpu_pkgs_json}" | jq --arg key "${pkg}" --argjson val "${pkg_details}" '.[$key] = $val')
  done
  env_details=$(echo "${env_details}" | jq --argjson val "${gpu_pkgs_json}" '.gpu_packages = $val')

  local jupyter_kernel_found=false
  local kernel_path="${conda_base}/envs/${conda_env}/share/jupyter/kernels"
  if [[ "${conda_env}" == "base" ]]; then kernel_path="${conda_base}/share/jupyter/kernels"; fi
  if [[ -d "${kernel_path}" ]] && [[ -n "$(ls -A "${kernel_path}" 2>/dev/null)" ]]; then
    jupyter_kernel_found=true
  fi
  if ! ${jupyter_kernel_found} && [[ -d "${HOME}/.local/share/jupyter/kernels" ]]; then
    local env_bin_path="${conda_base}/envs/${conda_env}/bin/python"
    if [[ "${conda_env}" == "base" ]]; then env_bin_path="${conda_base}/bin/python"; fi
    if grep -qF "${env_bin_path}" ${HOME}/.local/share/jupyter/kernels/*/kernel.json 2>/dev/null; then
      jupyter_kernel_found=true
    fi
  fi
  env_details=$(echo "${env_details}" | jq --arg val "${jupyter_kernel_found}" '.jupyter_kernel = ($val == "true")')
  echo "${env_details}"
}

function detect_aiml_environments() {
  local aiml_envs="{}"
  local output_file="$(mktemp /tmp/detect_aiml_environments.XXXXXX.json)"

  # System Environment
  local system_info="{}"
  system_info=$(echo "${system_info}" | jq --argjson val "$(check_system_cuda_repo)" '.cuda = $val')
  system_info=$(echo "${system_info}" | jq --argjson val "$(check_system_gpu_packages)" '.gpu_packages = $val')
  aiml_envs=$(echo "${aiml_envs}" | jq --argjson val "${system_info}" '.system = $val')

  # Conda Environments
  local conda_base
  conda_base=$(get_conda_base)
  if [[ -n "${conda_base}" ]]; then
    if ! command -v conda &>/dev/null; then
      if [[ -f "${conda_base}/etc/profile.d/conda.sh" ]]; then
        source "${conda_base}/etc/profile.d/conda.sh"
      fi
    fi
  fi

  if command -v conda &>/dev/null; then
    run_mamba_search
    local available_map_json=$(parse_mamba_search)

    local preferred_env
    preferred_env=$(get_metadata_attribute "gpu-conda-env" "dpgce")
    local conda_envs_path="${conda_base}/envs"
    local primary_env="base"

    if [[ -d "${conda_envs_path}/${preferred_env}" ]]; then
      primary_env="${preferred_env}"
    fi
    aiml_envs=$(echo "${aiml_envs}" | jq --arg key "primary_conda_env" --arg val "${primary_env}" '. + {($key): $val}')

    local envs_to_check=()
    if [[ -d "${conda_envs_path}" ]]; then
      for env_dir in "${conda_envs_path}"/*; do
        if [[ -d "${env_dir}" ]]; then
          envs_to_check+=("$(basename "${env_dir}")")
        fi
      done
    fi
    envs_to_check+=("base")
    # Deduplicate envs_to_check
    envs_to_check=($(printf "%s\n" "${envs_to_check[@]}" | sort -u))

    for env in "${envs_to_check[@]}"; do
      local env_path="${conda_base}/envs/${env}"
      if [[ "${env}" == "base" ]]; then env_path="${conda_base}"; fi
      if [[ -d "${env_path}" ]]; then
        local env_details=$(_check_conda_env_details "${env}" "${available_map_json}")
        if [[ -n "${env_details}" ]] && [[ "${env_details}" != "{}" ]]; then
          aiml_envs=$(echo "${aiml_envs}" | jq --arg key "conda_${env}" --argjson val "${env_details}" '.[$key] = $val')
        fi
      fi
    done
  fi
  echo "${aiml_envs}" | jq -c . > "${output_file}"
  echo "${output_file}"
}
# --- Main Evaluation Function ---

function evaluate_gpu_setup() {
  export DEBIAN_FRONTEND=noninteractive
  # set -x # Enable for deep debugging

  ensure_required_packages || return 1
  ensure_xmlstarlet || echo "WARN: xmlstarlet is not available. Some GPU info might be missing." >&2
  ensure_jq || return 1

  # --- Source Conda ---
  local conda_base
  conda_base=$(get_conda_base)
  if [[ -n "${conda_base}" ]]; then
    if ! command -v conda &>/dev/null; then
      if [[ -f "${conda_base}/etc/profile.d/conda.sh" ]]; then
        source "${conda_base}/etc/profile.d/conda.sh"
      fi
    fi
  fi

  local jq_args=()
  local jq_filter_parts=()
  local tmp_files_to_delete=()

  function add_json_part() {
    local key="$1"
    local value="$2"
    local type="${3:-string}" # string, number, boolean, json, jsonfile

    case "${type}" in
      string)
        jq_args+=("--arg" "${key}" "${value}")
        ;;
      number)
        jq_args+=("--argjson" "${key}" "${value}")
        ;;
      boolean)
        jq_args+=("--argjson" "${key}" "${value}")
        ;;
      json)
        jq_args+=("--argjson" "${key}" "${value}")
        ;;
      jsonfile)
        jq_args+=("--argfile" "${key}" "${value}")
        tmp_files_to_delete+=("${value}")
        ;;
    esac
    # Construct the filter part to assign the jq variable to the key
    jq_filter_parts+=(".[\"${key}\"] = \$${key}")
  }

  function finalize_json() {
    local jq_filter
    if [ ${#jq_filter_parts[@]} -gt 0 ]; then
      # Join parts with ' | ' to chain the assignments
      jq_filter=$(printf '%s | ' "${jq_filter_parts[@]}")
      # Remove the trailing ' | '
      jq_filter=${jq_filter% | }
    else
      jq_filter="."
    fi

    echo "Evaluation results written to ${EVALUATION_OUTPUT_FILE}" >&2
    # Start with n (null) and apply the filter parts
    jq -n -c "${jq_args[@]}" "${jq_filter}" | tee "${EVALUATION_OUTPUT_FILE}"
    rm -f "${tmp_files_to_delete[@]}" >/dev/null 2>&1
  }

  # --- Collect all data first ---
  local secure_boot=$(get_secure_boot_status)
  local proxy_settings=$(get_proxy_settings)
  local gpu_count=$(get_sysfs_gpu_count)

  # --- Metadata for conditional logic ---
  local dataproc_image_version=$(get_metadata_attribute dataproc-image-version "")
  if [[ -z "${dataproc_image_version}" ]] && [[ -f /etc/environment ]]; then
    source /etc/environment
    dataproc_image_version="${DATAPROC_IMAGE_VERSION:-}"
  fi
  local dataproc_role=$(/usr/share/google/get_metadata_value attributes/dataproc-role || echo "Unknown")
  local rapids_runtime=$(get_metadata_attribute 'rapids-runtime' 'SPARK')
  local install_gpu_agent=$(get_metadata_attribute 'install-gpu-agent' 'false')

  # --- OS Info ---
  local os_id_val=$(os_id)
  local os_version_val=$(os_version)
  local os_vercat_val=$(os_vercat)
  local os_info=$(jq -n --arg id "${os_id_val}" --arg version "${os_version_val}" --arg vercat "${os_vercat_val}" '{id: $id, version: $version, vercat: $vercat}')

  add_json_part "os" "${os_info}" "json"
  add_json_part "dataproc_image_version" "${dataproc_image_version}" "string"
  add_json_part "dataproc_role" "${dataproc_role}" "string"
  add_json_part "rapids_runtime" "${rapids_runtime}" "string"
  add_json_part "install_gpu_agent" "${install_gpu_agent}" "boolean"
  add_json_part "secure_boot" "${secure_boot}" "string"
  add_json_part "proxy_settings" "${proxy_settings}" "string"

  # --- Hadoop/Spark Config Checks ---
  local hadoop_configs_file=$(check_hadoop_configs)
  add_json_part "hadoop_config" "${hadoop_configs_file}" "jsonfile"

  if [[ "${gpu_count}" -eq 0 ]]; then
    add_json_part "gpu_count" "0" "number"
    add_json_part "evaluation_result" "NONE" "string"
    finalize_json
    return 0;
  fi

  local gpu_model=$(summarize_gpu_models "$(get_sysfs_gpu_devices)")
  local aiml_envs_file=$(detect_aiml_environments)
  local gpu_jars_file=$(find_gpu_spark_jars)

  # --- Build JSON parts in order ---
  add_json_part "aiml_environments" "${aiml_envs_file}" "jsonfile"
  add_json_part "gpu_spark_jars" "${gpu_jars_file}" "jsonfile"

  if [[ "${secure_boot}" == "enabled" ]]; then
    local PSN=$(get_metadata_attribute private_secret_name)
    local PUBSN=$(get_metadata_attribute public_secret_name)
    add_json_part "private_secret_name_set" $([[ -n "${PSN}" ]] && echo true || echo false) "boolean"
    if [[ -n "${PSN}" ]]; then
      add_json_part "public_secret_name_set" $([[ -n "${PUBSN}" ]] && echo true || echo false) "boolean"
      if fetch_public_mok; then
        add_json_part "public_secret_access" "OK" "string"
        add_json_part "mok_key_trusted" "$(is_signing_key_trusted && echo "Trusted" || echo "NOT FOUND")" "string"
      else
        add_json_part "public_secret_access" "FAILED" "string"
        add_json_part "mok_key_trusted" "Unknown" "string"
      fi
    fi
  fi

  if ! detect_nvidia_driver; then
     add_json_part "gpu_count" "${gpu_count}" "number"
     add_json_part "gpu_model" "${gpu_model}" "string"
     add_json_part "nvidia_driver_status" "Not Detected" "string"
     add_json_part "evaluation_result" "GPU_INSTANCE_ONLY" "string"
     finalize_json
     return 0;
  fi

  local nvidia_module_signature=$(get_nvidia_module_signature_status)

  if ! dump_nvidia_smi_xml; then
     add_json_part "gpu_count" "${gpu_count}" "number"
     add_json_part "gpu_model" "${gpu_model}" "string"
     add_json_part "nvidia_driver_status" "CRITICAL - nvidia-smi failed to query XML." "string"
     add_json_part "nvidia_module_signature" "${nvidia_module_signature}" "string"
     add_json_part "evaluation_result" "DRIVER_FAILURE" "string"
     finalize_json
     return 1;
  fi

  add_json_part "gpu_count" "$(get_gpu_count_from_xml)" "number"
  add_json_part "gpu_model" "$(summarize_gpu_models "$(get_gpu_models_from_xml)")" "string"

  local driver_version=$(get_driver_version_from_xml)
  add_json_part "nvidia_driver_status" "OK" "string"
  add_json_part "nvidia_driver_version" "${driver_version}" "string"
  add_json_part "nvidia_module_signature" "${nvidia_module_signature}" "string"

  export PATH=/usr/local/cuda/bin:${PATH}
  local cuda_version=$(get_cuda_version)
  local system_cuda_toolkit=$(jq -r '.system.gpu_packages.cudatoolkit.installed_version' "${aiml_envs_file}")
  local cuda_toolkit_version_val
  if [[ -z "${cuda_version}" ]]; then
    if [[ "${system_cuda_toolkit}" != "null" ]]; then
        cuda_version="${system_cuda_toolkit}"
        cuda_toolkit_version_val="${cuda_version} (system)"
    else
        cuda_toolkit_version_val="Not Detected"
        cuda_version=""
    fi
  else
    cuda_toolkit_version_val="${cuda_version}"
  fi
  add_json_part "cuda_toolkit_version" "${cuda_toolkit_version_val}" "string"

  local result="UNKNOWN"
  local conda_gpu_detected=$(jq -e '.[] | select(type == "object") | .gpu_packages | values | any(.installed == true)' "${aiml_envs_file}" >/dev/null && echo true || echo false)
  local spark_rapids_jar_detected=$(jq -e '.["rapids-spark"] != null' "${gpu_jars_file}" >/dev/null && echo true || echo false)
  local system_cuda_repo_ok=$(jq -e '.system.cuda.cuda_repo_file != "Not Found" and .system.cuda.cuda_keyring_file != "Not Found"' "${aiml_envs_file}" >/dev/null && echo true || echo false)

  if [[ -n "${driver_version}" ]]; then
    if [[ -n "${cuda_version}" ]]; then
      if [[ "${spark_rapids_jar_detected}" == "true" ]]; then
        if [[ "${conda_gpu_detected}" == "true" ]]; then result="SPARK_RAPIDS_BASE_WITH_CONDA"; else result="SPARK_RAPIDS_BASE"; fi
      else
        if [[ "${conda_gpu_detected}" == "true" ]]; then result="CUDA_DETECTED_WITH_CONDA"; else result="CUDA_DETECTED"; fi
      fi
    elif [[ "${system_cuda_repo_ok}" == "true" ]]; then
      result="DRIVERS_AND_REPO_CONFIGURED"
    else
      result="DRIVERS_INSTALLED"
    fi
  else
    result="GPU_INSTANCE_ONLY"
  fi
  add_json_part "evaluation_result" "${result}" "string"

  finalize_json
}
# --- GPU Detection Functions End ---
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
else
  echo "Error: Your Spark version is not supported. Please upgrade Spark to one of the supported versions."
  exit 1
fi

# Update SPARK RAPIDS config
readonly DEFAULT_SPARK_RAPIDS_VERSION="25.12.0"
readonly SPARK_RAPIDS_VERSION=$(get_metadata_attribute 'spark-rapids-version' ${DEFAULT_SPARK_RAPIDS_VERSION})
readonly XGBOOST_VERSION=$(get_metadata_attribute 'xgboost-version' ${DEFAULT_XGBOOST_VERSION})

# Fetch instance roles and runtime
readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly RUNTIME=$(get_metadata_attribute 'rapids-runtime' 'SPARK')

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

function install_gpu_xgboost() {
  local -r dmlc_repo_url='https://repo.maven.apache.org/maven2/ml/dmlc'

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-spark-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-spark-gpu_2.12-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${dmlc_repo_url}/xgboost4j-gpu_2.12/${XGBOOST_VERSION}/xgboost4j-gpu_2.12-${XGBOOST_VERSION}.jar" \
    -P /usr/lib/spark/jars/
}

function check_spark_rapids_jar() {
  local jars_found
  jars_found=$(ls /usr/lib/spark/jars/rapids-4-spark_*.jar 2>/dev/null | wc -l)
  if [[ $jars_found -gt 0 ]]; then
    echo "RAPIDS Spark plugin JAR found"
    return 0
  else
    echo "RAPIDS Spark plugin JAR not found"
    return 1
  fi
}

function remove_spark_rapids_jar() {
  rm -f /usr/lib/spark/jars/rapids-4-spark_*.jar
  echo "Existing RAPIDS Spark plugin JAR removed successfully"
}

function install_spark_rapids() {

  local -r nvidia_repo_url='https://repo1.maven.org/maven2/com/nvidia'

  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${nvidia_repo_url}/rapids-4-spark_2.12/${SPARK_RAPIDS_VERSION}/rapids-4-spark_2.12-${SPARK_RAPIDS_VERSION}.jar" \
    -P /usr/lib/spark/jars/
}

function configure_spark() {
  if [[ "${SPARK_VERSION}" == "3"* ]]; then
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

    # Unhold NVIDIA packages to allow upgrades (see issue #1321)
    unhold_nvidia_packages

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
  # If the RAPIDS Spark RAPIDS JAR is already installed (common on ML images), replace it with the requested version
  # ML images by default have Spark RAPIDS and GPU drivers installed
  if check_spark_rapids_jar; then
    # This ensures the cluster always uses the desired RAPIDS version, even if a default is present
    remove_spark_rapids_jar
    install_spark_rapids
    echo "RAPIDS Spark RAPIDS JAR replaced successfully"
  else
    # Install GPU drivers and setup SPARK RAPIDS JAR for non-ML images
    if is_debian && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
      remove_old_backports
    fi
    check_os_and_secure_boot
    setup_gpu_yarn
    if [[ "${RUNTIME}" == "SPARK" ]]; then
      install_spark_rapids
      install_gpu_xgboost
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
  fi
}

main
