#!/bin/bash

set -exo pipefail

# Space-separated list of Conda channels to add before installing packages
readonly CHANNELS=$(/usr/share/google/get_metadata_value attributes/CONDA_CHANNELS || true)

# Space-separated list of Conda packages to install
readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || true)

function main() {
  if [[ -n "${CHANNELS}" ]]; then
    for channel in ${CHANNELS}; do
      conda config --add channels "${channel}"
    done
  fi

  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify CONDA_PACKAGES metadata key"
    exit 1
  fi
  conda install ${PACKAGES}
}

main
