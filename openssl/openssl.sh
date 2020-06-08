#!/bin/bash

readonly NOT_SUPPORTED_MESSAGE="OpenSSL initialization action is not supported on Dataproc 2.0+.
OpenSSL is installed by default"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

set -euxo pipefail

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

# This init action is not necessary on debian 9 (stretch)
if [[ "$(lsb_release -sc)" == "jessie" ]]; then
  update_apt_get
  apt-get install -t jessie-backports -y openssl
fi
