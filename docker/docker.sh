#!/usr/bin/env bash

set -euxo pipefail

# TODO: Allow this to be configured by metadata.
readonly DOCKER_VERSION='18.06.0~ce~3-0~debian'

function is_master() {
  local role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ "$role" == 'Master' ]] ; then
    true
  else
    false
  fi
}

function get_docker_gpg() {
  curl -fsSL https://download.docker.com/linux/debian/gpg
}

function update_apt_get() {
  for ((i = 0; i < 10; i++)) ; do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_docker() {
  get_docker_gpg | apt-key add -
  add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
  update_apt_get
  apt-get install -y docker-ce="${DOCKER_VERSION}"
}

function configure_docker() {
  # The installation package should create `docker` group.
  usermod -aG docker yarn
  systemctl enable docker
  # Restart YARN daemons to pick up new group without restarting nodes.
  if is_master ; then
    systemctl restart hadoop-yarn-resourcemanager
  else
    systemctl restart hadoop-yarn-nodemanager
  fi
}

function main() {
  install_docker
  configure_docker
}

main "$@"
