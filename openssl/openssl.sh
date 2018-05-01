#!/bin/bash

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

update_apt_get
apt-get install -t jessie-backports -y openssl
