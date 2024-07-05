#!/bin/bash

set -exo pipefail

readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)
readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

function remove_old_backports {
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  oldstable=$(curl -s https://deb.debian.org/debian/dists/oldstable/Release | awk '/^Codename/ {print $2}');
  stable=$(curl -s https://deb.debian.org/debian/dists/stable/Release | awk '/^Codename/ {print $2}');

  matched_files=( $(grep -rsil '\-backports' /etc/apt/sources.list*||:) )
  if [[ -n "$matched_files" ]]; then
    for filename in "${matched_files[@]}"; do
      grep -e "$oldstable-backports" -e "$stable-backports" "$filename" || \
        sed -i -e 's/^.*-backports.*$//' "$filename"
    done
  fi
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
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

  echo "Installing python-pip..."
  run_with_retry apt update
  run_with_retry apt install python-pip -y
}

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify PIP_PACKAGES metadata key"
    exit 1
  fi
  if [[ ${OS_NAME} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    remove_old_backports
  fi
  install_pip
  run_with_retry pip install --upgrade ${PACKAGES}
}

main

