#!/bin/bash
#  Copyright 2015 Google, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  This initialization action installs Ganglia, a distributed monitoring system.

set -x -e

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function install_ganglia_dependencies() {
  # Install dependencies needed for ganglia
  sed -e "/name = \"unspecified\" /s/unspecified/${master}/" -i /etc/ganglia/gmond.conf
  sed -e '/mcast_join /s/^  /  #/' -i /etc/ganglia/gmond.conf
  sed -e '/bind /s/^  /  #/' -i /etc/ganglia/gmond.conf
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    rrdtool \
    gmetad \
    ganglia-webfrontend || err 'Unable to install packages'

  ln -s /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled/ganglia.conf
  sed -i "s/my cluster/${master}/" /etc/ganglia/gmetad.conf
  sed -e '/udp_send_channel {/a\  host = localhost' -i /etc/ganglia/gmond.conf
  systemctl restart ganglia-monitor &&
  systemctl restart gmetad &&
  systemctl restart apache2

}

function main() {
  local role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
  local master=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

  update_apt_get || err 'Unable to update apt-get'
  apt-get install -y ganglia-monitor

  if [[ "${role}" == 'Master' ]]; then
    # Only run on the master node
    install_ganglia_dependencies || err 'Installing dependencies for Ganglia failed'
  else
    sed -e "/udp_send_channel {/a\  host = ${master}" -i /etc/ganglia/gmond.conf
    sed -i '/udp_recv_channel {/,/}/d' /etc/ganglia/gmond.conf
    systemctl restart ganglia-monitor
  fi
}

main
