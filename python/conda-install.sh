#!/bin/bash

set -exo pipefail

readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/CONDA_PACKAGES || true)

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify CONDA_PACKAGES metadata key" 
    exit 1
  fi
  conda install ${PACKAGES}
}

main

