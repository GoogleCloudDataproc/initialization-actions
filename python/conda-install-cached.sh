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

# This script installs Conda packages, using GCS to cache the resulting environment.

set -exo pipefail

readonly CHANNELS=$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || true)
readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || true)
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

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function get_cache_key() {
  local sorted_packages=$(echo "${PACKAGES}" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)
  local sorted_channels=$(echo "${CHANNELS}" | tr ' ' '\n' | sort | tr '\n' ' ' | xargs)
  # Only hash the sorted packages and channels, environment details are in the path
  echo "${sorted_packages}_${sorted_channels}" | sha256sum | awk '{print $1}'
}

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify CONDA_PACKAGES metadata key"
    exit 1
  fi

  if [[ -z "${CACHE_BUCKET}" ]]; then
    echo "ERROR: Must specify CACHE_BUCKET metadata key for cached installation"
    exit 1
  fi

  # Configure channels first
  if [[ -n "${CHANNELS}" ]]; then
    for channel in ${CHANNELS}; do
      conda config --add channels "${channel}"
    done
  fi

  local conda_path=$(which conda)
  local env_path

  if [[ -n "${TARGET_ENV_PATH}" ]]; then
    env_path="${TARGET_ENV_PATH}"
    echo "Using target environment path from metadata: ${env_path}"
  else
    env_path=$(dirname $(dirname "${conda_path}"))
    if [[ "${env_path}" == "/usr" || "${env_path}" == "/usr/local" || "${env_path}" == "/" ]]; then
      echo "ERROR: Refusing to cache system-wide environment path: ${env_path}."
      echo "Caching the entire system directory is unsafe. Please specify a target environment path using TARGET_ENV_PATH."
      exit 1
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
  # Follow design doc storage layout: gs://<CACHE_BUCKET>/dataproc-caching/conda/<OS_DATAPROC_VER>_<ARCH>/<HASH_OR_KEY>.tar.gz
  local gcs_tarball="gs://${CACHE_BUCKET}/dataproc-caching/conda/${OS_NAME}_${DATAPROC_IMAGE_VERSION}_${arch}/${cache_key}.tar.gz"
  local local_tarball="/tmp/${cache_key}.tar.gz"

  set +e
  gsutil -q stat "${gcs_tarball}"
  local cache_exists_code=$?
  set -e

  if [[ ${cache_exists_code} -eq 0 ]]; then
    local file_time=$(gsutil stat "${gcs_tarball}" | grep -oP 'Creation time:\s*\K.*' || echo "")
    local file_epoch=""
    if [[ -n "${file_time}" ]]; then
      file_epoch=$(date -u -d "${file_time}" +%s 2>/dev/null || echo "")
    fi

    if [[ -n "${file_epoch}" ]]; then
      local now_epoch=$(date -u +%s)
      local age=$((now_epoch - file_epoch))

      if (( age <= CACHE_TIMEOUT )); then
        echo "Cache hit for key ${cache_key} (age: ${age}s). Unpacking from ${gcs_tarball}"
        if gsutil cat "${gcs_tarball}" | tar -C "${env_path}" -xz; then
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
      if gsutil cat "${gcs_tarball}" | tar -C "${env_path}" -xz; then
        return 0
      else
        echo "WARNING: Failed to unpack cache. Falling back to rebuilding."
      fi
    fi
  fi

  echo "Cache miss for key ${cache_key}. Checking for concurrent builds."

  set +e
  local building_output=$(gsutil -q stat "${gcs_tarball}.building" 2>/dev/null)
  local sentinel_exists_code=$?
  set -e

  if [[ ${sentinel_exists_code} -eq 0 ]]; then
    echo "Another node is building this environment. Waiting..."
    local wait_start=$(date +%s)
    local timeout="${CACHE_TIMEOUT}"
    while gsutil -q stat "${gcs_tarball}.building" > /dev/null 2>&1; do
      if gsutil -q stat "${gcs_tarball}" > /dev/null 2>&1; then
        echo "Cache file appeared while waiting. Skipping build."
        if gsutil cat "${gcs_tarball}" | tar -C "${env_path}" -xz; then
          return 0
        fi
        echo "WARNING: Failed to unpack appeared cache. Proceeding to build."
        break
      fi
      local now=$(date +%s)
      if (( now - wait_start > timeout )); then
        echo "Timeout waiting for concurrent build. Proceeding to build myself."
        break
      fi
      echo "Waiting 30 seconds..."
      sleep 30
    done

    # Double check if cache appeared just after sentinel removal
    if gsutil -q stat "${gcs_tarball}" > /dev/null 2>&1; then
      echo "Cache file appeared. Skipping build."
      if gsutil cat "${gcs_tarball}" | tar -C "${env_path}" -xz; then
        return 0
      fi
      echo "WARNING: Failed to unpack appeared cache. Proceeding to build."
    fi
  fi

  echo "Proceeding to build and cache environment."

  local -a pack_arr
  read -r -a pack_arr <<< "${PACKAGES}"

  touch "${local_tarball}.building"
  
  set +e
  gsutil cp "${local_tarball}.building" "${gcs_tarball}.building"
  local sentinel_upload_code=$?
  set -e

  if [[ ${sentinel_upload_code} -ne 0 ]]; then
    echo "WARNING: Failed to upload sentinel to GCS. GCS might be unreachable."
    echo "Falling back to standard non-cached installation."
    conda install -y -p "${env_path}" "${pack_arr[@]}"
    rm -f "${local_tarball}.building"
    return 0
  fi

  # Ensure we clean up sentinel on exit if we uploaded it
  trap 'gsutil rm "${gcs_tarball}.building" || true; rm -f "${local_tarball}.building"' EXIT

  # Perform standard installation
  conda install -y -p "${env_path}" "${pack_arr[@]}"

  echo "Packaging environment."
  # We package the contents of the environment path
  pushd "${env_path}"
  tar czf "${local_tarball}" .
  popd

  echo "Uploading to GCS."
  set +e
  gsutil cp "${local_tarball}" "${gcs_tarball}"
  local cache_upload_code=$?
  set -e

  if [[ ${cache_upload_code} -ne 0 ]]; then
    echo "WARNING: Failed to upload cache to GCS."
  fi

  echo "Cleaning up."
  trap - EXIT
  gsutil rm "${gcs_tarball}.building" || true
  rm -f "${local_tarball}" "${local_tarball}.building"

  echo "Done."
}

main
