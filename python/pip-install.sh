#!/bin/bash

set -exo pipefail

readonly PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES || true)

function install_pip() {
  if command -v pip >/dev/null; then
    echo "pip is already installed."
    return 0
  fi

  if command -v easy_install >/dev/null; then
    echo "Installing pip with easy_install..."
    easy_install pip
    return 0
  fi

  echo "Installing python-pip..."
  apt update
  apt install python-pip -y
}

function main() {
  if [[ -z "${PACKAGES}" ]]; then
    echo "ERROR: Must specify PIP_PACKAGES metadata key"
    exit 1
  fi

  install_pip
  pip install --upgrade ${PACKAGES}
}

main
