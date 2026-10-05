#!/bin/bash

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

# This script installs Python packages via pip, using GCS to cache the resulting environment.

set -exo pipefail

readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)
CACHE_BUCKET=$(/usr/share/google/get_metadata_value attributes/CACHE_BUCKET || true)
if [[ -z "${CACHE_BUCKET}" ]]; then
  CACHE_BUCKET=$(/usr/share/google/get_metadata_value attributes/dataproc-temp-bucket || true)
  if [[ -n "${CACHE_BUCKET}" ]]; then
    echo "CACHE_BUCKET not specified, defaulting to dataproc-temp-bucket: ${CACHE_BUCKET}"
  fi
fi
readonly CACHE_BUCKET
readonly CACHE_KEY_OVERRIDE=$(/usr/share/google/get_metadata_value attributes/CACHE_KEY_OVERRIDE || true)
CACHE_TIMEOUT=$(/usr/share/google/get_metadata_value attributes/CACHE_TIMEOUT || true)
if [[ -z "${CACHE_TIMEOUT}" ]]; then
  CACHE_TIMEOUT=315360000 # 10 years (effectively infinite)
fi
readonly CACHE_TIMEOUT
readonly TARGET_ENV_PATH=$(/usr/share/google/get_metadata_value attributes/TARGET_ENV_PATH || true)
readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')

GCS_CMD="gsutil"
if gcloud --help >/dev/null 2>&1 && gcloud storage --help >/dev/null 2>&1; then
  GCS_CMD="gcloud storage"
fi
readonly GCS_CMD

function gcs_exists() {
  local uri=$1
  if [[ "${GCS_CMD}" == "gcloud storage" ]]; then
    gcloud storage objects describe "${uri}" >/dev/null 2>&1
  else
    gsutil -q stat "${uri}"
  fi
}

function gcs_file_epoch() {
  local uri=$1
  if [[ "${GCS_CMD}" == "gcloud storage" ]]; then
    local file_time=$(gcloud storage objects describe "${uri}" --format="value(timeCreated)" 2>/dev/null || echo "")
    if [[ -n "${file_time}" ]]; then
      date -u -d "${file_time}" +%s 2>/dev/null || echo ""
    fi
  else
    local file_time=$(gsutil stat "${uri}" | grep -oP 'Creation time:\s*\K.*' || echo "")
    if [[ -n "${file_time}" ]]; then
      date -u -d "${file_time}" +%s 2>/dev/null || echo ""
    fi
  fi
}

function gcs_cat() {
  local uri=$1
  if [[ "${GCS_CMD}" == "gcloud storage" ]]; then
    gcloud storage cat "${uri}"
  else
    gsutil cat "${uri}"
  fi
}

function gcs_atomic_upload() {
  local local_path=$1
  local gcs_uri=$2
  if [[ "${GCS_CMD}" == "gcloud storage" ]]; then
    gcloud storage cp --if-generation-match=0 "${local_path}" "${gcs_uri}" 2>/dev/null
  else
    gsutil -h "x-goog-if-generation-match:0" cp "${local_path}" "${gcs_uri}" 2>/dev/null
  fi
}

function gcs_cp() {
  local local_path=$1
  local gcs_uri=$2
  if [[ "${GCS_CMD}" == "gcloud storage" ]]; then
    gcloud storage cp "${local_path}" "${gcs_uri}"
  else
    gsutil cp "${local_path}" "${gcs_uri}"
  fi
}

function gcs_rm() {
  local uri=$1
  if [[ "${GCS_CMD}" == "gcloud storage" ]]; then
    gcloud storage rm "${uri}"
  else
    gsutil rm "${uri}"
  fi
}

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function remove_old_backports() {
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  local oldstable=$(curl -s --connect-timeout 5 --max-time 10 https://deb.debian.org/debian/dists/oldstable/Release | awk '/^Codename/ {print $2}');
  local stable=$(curl -s --connect-timeout 5 --max-time 10 https://deb.debian.org/debian/dists/stable/Release | awk '/^Codename/ {print $2}');

  local matched_files=( $(grep -rsil '\-backports' /etc/apt/sources.list*||:) )
  if [[ -n "$matched_files" ]]; then
    for filename in "${matched_files[@]}"; do
      grep -e "$oldstable-backports" -e "$stable-backports" "$filename" || \
        sed -i -e 's/^.*-backports.*$//' "$filename"
    done
  fi
}

function run_with_retry() {
  local -r cmd=("$@")
  for ((i = 0; i < 10; i++)); do
    if "${cmd[@]}"; then
      return 0
    fi
    sleep 5
  done
  err "Failed to run command: ${cmd[*]}"
}

function install_pip() {
  if command -v pip >/dev/null; then
    echo "pip is already installed."
    return 0
  fi

  if command -v easy_install >/dev/null; then
    echo "Installing pip with easy_install..."
    run_with_retry easy_install pip
    return 0
  fi

  echo "Installing python3-pip..."
  run_with_retry apt update
  run_with_retry apt install python3-pip -y
}

function get_cache_key() {
  local sorted_packages=$(echo "${PACKAGES}" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)
  # Only hash the sorted packages, environment details are in the path
  echo "${sorted_packages}" | sha256sum | awk '{print $1}'
}

function ensure_venv() {
  if python3 -m venv --help >/dev/null 2>&1; then
    echo "venv is available."
    return 0
  fi

  echo "Installing venv..."
  if [[ "${OS_NAME}" == "debian" || "${OS_NAME}" == "ubuntu" ]]; then
    run_with_retry apt update
    run_with_retry apt install python3-venv -y
  elif [[ "${OS_NAME}" == "rocky" ]]; then
    run_with_retry yum install -y python3-virtualenv || run_with_retry dnf install -y python3-virtualenv
  fi
}

function merge_isolated_env() {
  local venv_path=$1
  echo "Merging isolated environment back to system Python"
  local system_site_packages=$(/usr/bin/python3 -c "import site; print(site.getsitepackages()[0])" 2>/dev/null)
  local venv_site_packages=$(find "${venv_path}/lib" -maxdepth 2 -type d -name "site-packages" 2>/dev/null | head -n 1)
  if [[ -n "${system_site_packages}" && -n "${venv_site_packages}" ]]; then
    cp -a "${venv_site_packages}/." "${system_site_packages}/"
    if [[ -d "${venv_path}/bin" ]]; then
      find "${venv_path}/bin" -type f ! -name "python*" ! -name "pip*" ! -name "easy_install*" ! -name "activate*" -exec cp -a {} /usr/local/bin/ \;
    fi
  else
    echo "ERROR: Could not find system or venv site-packages directory"
    return 1
  fi
}

function unpack_and_merge() {
  local tarball=$1
  local target_path=$2
  local original_path=$3
  local is_isolated=$4

  mkdir -p "${target_path}"
  if gcs_cat "${tarball}" | tar -C "${target_path}" -xz; then
    if [[ "${is_isolated}" == "true" ]]; then
      merge_isolated_env "${target_path}"
    fi
    return 0
  fi
  return 1
}

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify PIP_PACKAGES metadata key"
    exit 1
  fi

  if [[ -z "${CACHE_BUCKET}" ]]; then
    echo "ERROR: Must specify CACHE_BUCKET metadata key for cached installation"
    exit 1
  fi

  local env_path
  local pip_bin
  local original_env_path=""
  local isolated_env_path="/opt/pip-isolated-env"
  local isolated="false"

  if [[ -n "${TARGET_ENV_PATH}" ]]; then
    env_path="${TARGET_ENV_PATH}"
    pip_bin="${env_path}/bin/pip"
    echo "Using target environment path from metadata: ${env_path}"
  else
    if [[ "${OS_NAME}" == "debian" ]] && [[ -n "${DATAPROC_IMAGE_VERSION}" ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
      remove_old_backports
    fi
    install_pip
    local pip_path=$(which pip)
    env_path=$(dirname $(dirname "${pip_path}"))
    if [[ "${env_path}" == "/usr" || "${env_path}" == "/usr/local" || "${env_path}" == "/" ]]; then
      echo "WARNING: Inferred system-wide environment path: ${env_path}."
      echo "Switching to isolated venv for caching to avoid archiving system directories."
      ensure_venv
      original_env_path="${env_path}"
      env_path="${isolated_env_path}"
      pip_bin="${env_path}/bin/pip"
      isolated="true"
    else
      pip_bin="pip"
    fi
    echo "Inferred target environment path: ${env_path}"
  fi

  echo "Target environment path: ${env_path}"

  local cache_key
  if [[ -n "${CACHE_KEY_OVERRIDE}" ]]; then
    cache_key="${CACHE_KEY_OVERRIDE}"
    echo "Using manual cache key override: ${cache_key}"
  else
    cache_key=$(get_cache_key)
  fi

  local arch=$(uname -m)
  # Follow design doc storage layout: gs://<CACHE_BUCKET>/dataproc-caching/pip/<OS_DATAPROC_VER>_<ARCH>/<HASH_OR_KEY>.tar.gz
  local gcs_tarball="gs://${CACHE_BUCKET}/dataproc-caching/pip/${OS_NAME}_${DATAPROC_IMAGE_VERSION}_${arch}/${cache_key}.tar.gz"
  local local_tarball="/tmp/${cache_key}.tar.gz"

  set +e
  gcs_exists "${gcs_tarball}"
  local cache_exists_code=$?
  set -e

  if [[ ${cache_exists_code} -eq 0 ]]; then
    local file_epoch=$(gcs_file_epoch "${gcs_tarball}")

    if [[ -n "${file_epoch}" ]]; then
      local now_epoch=$(date -u +%s)
      local age=$((now_epoch - file_epoch))

      if (( age <= CACHE_TIMEOUT )); then
        echo "Cache hit for key ${cache_key} (age: ${age}s). Unpacking from ${gcs_tarball}"
        if unpack_and_merge "${gcs_tarball}" "${env_path}" "${original_env_path}" "${isolated}"; then
          echo "Cache unpacked successfully."
          return 0
        else
          echo "WARNING: Failed to unpack cache. Falling back to rebuilding."
        fi
      else
        echo "Cache expired for key ${cache_key} (age: ${age}s > ${CACHE_TIMEOUT}s). Rebuilding."
      fi
    else
      echo "Cache hit for key ${cache_key} (could not verify age). Unpacking."
      if unpack_and_merge "${gcs_tarball}" "${env_path}" "${original_env_path}" "${isolated}"; then
        return 0
      else
        echo "WARNING: Failed to unpack cache. Falling back to rebuilding."
      fi
    fi
  fi

  echo "Cache miss for key ${cache_key}. Checking for concurrent builds."

  while true; do
    set +e
    gcs_exists "${gcs_tarball}.building"
    local sentinel_exists_code=$?
    set -e

    if [[ ${sentinel_exists_code} -eq 0 ]]; then
      local sentinel_epoch=$(gcs_file_epoch "${gcs_tarball}.building")
      if [[ -n "${sentinel_epoch}" ]]; then
        local now_epoch=$(date -u +%s)
        local sentinel_age=$((now_epoch - sentinel_epoch))
        if (( sentinel_age > 1200 )); then
          echo "Found stale sentinel (age: ${sentinel_age}s > 1200s). Removing it."
          gcs_rm "${gcs_tarball}.building" || true
          sentinel_exists_code=1
        fi
      fi
    fi

    if [[ ${sentinel_exists_code} -eq 0 ]]; then
      echo "Another node is building this environment. Waiting..."
      local wait_start=$(date +%s)
      local timeout=1200
      while gcs_exists "${gcs_tarball}.building"; do
        if gcs_exists "${gcs_tarball}"; then
          echo "Cache file appeared while waiting. Skipping build."
          if unpack_and_merge "${gcs_tarball}" "${env_path}" "${original_env_path}" "${isolated}"; then
            return 0
          fi
          echo "WARNING: Failed to unpack the newly available cache. Proceeding to build."
          break
        fi
        local now=$(date +%s)
        if (( now - wait_start > timeout )); then
          echo "Timeout waiting for concurrent build. Removing stale sentinel and proceeding to build myself."
          gcs_rm "${gcs_tarball}.building" || true
          break
        fi
        echo "Waiting 30 seconds..."
        sleep 30
      done
      
      # Double check if cache appeared just after sentinel removal
      if gcs_exists "${gcs_tarball}"; then
        echo "Cache file appeared. Skipping build."
        if unpack_and_merge "${gcs_tarball}" "${env_path}" "${original_env_path}" "${isolated}"; then
          return 0
        fi
        echo "WARNING: Failed to unpack the newly available cache. Proceeding to build."
      fi
    fi

    echo "Proceeding to build and cache environment."
    
    touch "${local_tarball}.building"
    
    set +e
    gcs_atomic_upload "${local_tarball}.building" "${gcs_tarball}.building"
    local sentinel_upload_code=$?
    set -e

    if [[ ${sentinel_upload_code} -eq 0 ]]; then
      break
    fi

    # Check if the failure was due to a race condition (sentinel exists) or GCS being unreachable
    if ! gcs_exists "${gcs_tarball}.building"; then
      echo "WARNING: Failed to upload sentinel to GCS. GCS might be unreachable."
      echo "Falling back to standard non-cached installation."
      local -a pack_arr
      read -r -a pack_arr <<< "${PACKAGES}"
      run_with_retry "${pip_bin}" install --upgrade "${pack_arr[@]}"
      if [[ "${isolated}" == "true" ]]; then
        merge_isolated_env "${env_path}"
      fi
      rm -f "${local_tarball}.building"
      return 0
    fi

    echo "Lost the lock race to another node. Retrying..."
  done

  # Ensure we clean up sentinel on exit if we uploaded it
  trap 'gcs_rm "${gcs_tarball}.building" || true; rm -f "${local_tarball}.building"' EXIT

  # Perform standard installation
  local -a pack_arr
  read -r -a pack_arr <<< "${PACKAGES}"
  run_with_retry "${pip_bin}" install --upgrade "${pack_arr[@]}"

  if [[ "${isolated}" == "true" ]]; then
    merge_isolated_env "${env_path}"
  fi

  echo "Packaging environment."
  # We package the contents of the environment path
  pushd "${env_path}"
  tar czf "${local_tarball}" .
  popd

  echo "Uploading to GCS."
  set +e
  gcs_cp "${local_tarball}" "${gcs_tarball}"
  local cache_upload_code=$?
  set -e

  if [[ ${cache_upload_code} -ne 0 ]]; then
    echo "WARNING: Failed to upload cache to GCS."
  fi

  echo "Cleaning up."
  trap - EXIT
  gcs_rm "${gcs_tarball}.building" || true
  rm -f "${local_tarball}" "${local_tarball}.building"

  echo "Done."
}

main
