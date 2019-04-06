#!/bin/bash

set -exo pipefail

readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify PIP_PACKAGES metadata key" 
    exit 1
  fi

  easy_install pip
<<<<<<< HEAD
  pip install --upgrade "${PACKAGES}"
=======
  pip install --upgrade ${PACKAGES}
>>>>>>> upstream/master
}

main
