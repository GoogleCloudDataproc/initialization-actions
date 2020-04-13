#!/bin/bash

set -exo pipefail

# Space-separated list of Conda channels to add before installing packages
readonly CHANNELS=$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || true)

# Space-separated list of Conda packages to install
readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || true)

function main() {
  if [[ -z ${CHANNELS}" && -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify CONDA_CHANNELS and/or CONDA_PACKAGES metadata keys"
    exit 1
  fi

  if [[ -n "${CHANNELS}" ]]; then
    for channel in ${CHANNELS}; do
      conda config --add channels "${channel}"
    done
  fi

  if [[ -n "${PACKAGES}" ]]; then
    conda install ${PACKAGES}
  fi
}

main
