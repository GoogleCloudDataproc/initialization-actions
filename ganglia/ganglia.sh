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

set -euxo pipefail

function os_id()       ( set +x ;  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_version()  ( set +x ;  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_codename() ( set +x ;  grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs ; )

# For version (or real number) comparison
# if first argument is greater than or equal to, greater than, less than or equal to, or less than the second
# ( version_ge 2.0 2.1 ) evaluates to false
# ( version_ge 2.2 2.1 ) evaluates to true
function version_ge() ( set +x ;  [ "$1" = "$(echo -e "$1\n$2" | sort -V | tail -n1)" ] ; )
function version_gt() ( set +x ;  [ "$1" = "$2" ] && return 1 || version_ge $1 $2 ; )
function version_le() ( set +x ;  [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ] ; )
function version_lt() ( set +x ;  [ "$1" = "$2" ] && return 1 || version_le $1 $2 ; )

function define_os_comparison_functions() {

  readonly -A supported_os=(
    ['debian']="10 11 12"
    ['rocky']="8 9"
    ['ubuntu']="18.04 20.04 22.04"
  )

  # dynamically define OS version test utility functions
  if [[ "$(os_id)" == "rocky" ]];
  then _os_version=$(os_version | sed -e 's/[^0-9].*$//g')
  else _os_version="$(os_version)"; fi
  for os_id_val in 'rocky' 'ubuntu' 'debian' ; do
    eval "function is_${os_id_val}() ( set +x ;  [[ \"$(os_id)\" == '${os_id_val}' ]] ; )"

    for osver in $(echo "${supported_os["${os_id_val}"]}") ; do
      eval "function is_${os_id_val}${osver%%.*}() ( set +x ; is_${os_id_val} && [[ \"${_os_version}\" == \"${osver}\" ]] ; )"
      eval "function ge_${os_id_val}${osver%%.*}() ( set +x ; is_${os_id_val} && version_ge \"${_os_version}\" \"${osver}\" ; )"
      eval "function le_${os_id_val}${osver%%.*}() ( set +x ; is_${os_id_val} && version_le \"${_os_version}\" \"${osver}\" ; )"
    done
  done
  eval "function is_debuntu()  ( set +x ;  is_debian || is_ubuntu ; )"
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update > /dev/null ; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function setup_ganglia_host() {
  # Install dependencies needed for Ganglia host
  apt-get install -qq -y -o DPkg::Lock::Timeout=60 \
    rrdtool \
    gmetad \
    ganglia-webfrontend >/dev/null || err 'Unable to install packages'

  ln -sf /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled/ganglia.conf
  perl -pi -e "s:^data_source.*:data_source \"${master_hostname}\" localhost:g" /etc/ganglia/gmetad.conf
  sed -i '26s/ \$context_metrics \= \"\"\;/ \$context_metrics \= array\(\)\;/g' /usr/share/ganglia-webfrontend/cluster_view.php
  systemctl restart ganglia-monitor gmetad apache2
}

function setup_ganglia_worker() {
  # on single node instances, also configure ganglia-monitor
  sed -e "/deaf = no /s/no/yes/" -i /etc/ganglia/gmond.conf
  sed -i '/udp_recv_channel {/,/}/d' /etc/ganglia/gmond.conf
  systemctl restart ganglia-monitor
}

function repair_old_backports {
  if ! is_debuntu ; then return ; fi
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will use archive.debian.org for the oldoldstable repo

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  debdists="https://deb.debian.org/debian/dists"
  oldoldstable=$(curl -s "${debdists}/oldoldstable/Release" | awk '/^Codename/ {print $2}');
  oldstable=$(   curl -s "${debdists}/oldstable/Release"    | awk '/^Codename/ {print $2}');
  stable=$(      curl -s "${debdists}/stable/Release"       | awk '/^Codename/ {print $2}');

  matched_files=( $(test -d /etc/apt && grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  for filename in "${matched_files[@]}"; do
    # Fetch from archive.debian.org for ${oldoldstable}-backports
    perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                  {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
  done
}

function main() {
  local master_hostname=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
  local cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

  export DEBIAN_FRONTEND=noninteractive

  OS=$(. /etc/os-release && echo "${ID}")

  define_os_comparison_functions

  # Detect dataproc image version
  SPARK_VERSION="$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)"
  readonly SPARK_VERSION

  if (! test -v DATAPROC_IMAGE_VERSION) ; then
    if test -v DATAPROC_VERSION ; then
      DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
    else
      if   version_lt "${SPARK_VERSION}" "3.2" ; then DATAPROC_IMAGE_VERSION="2.0"
      elif version_lt "${SPARK_VERSION}" "3.4" ; then DATAPROC_IMAGE_VERSION="2.1"
      elif version_lt "${SPARK_VERSION}" "3.6" ; then DATAPROC_IMAGE_VERSION="2.2"
      else echo "Unknown dataproc image version" ; exit 1 ; fi
    fi
  fi

  if [[ ${OS} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    repair_old_backports
  fi

  update_apt_get > /dev/null || err 'Unable to update apt-get'
  apt-get install -qq -y -o DPkg::Lock::Timeout=60 ganglia-monitor > /dev/null

  sed -e "/send_metadata_interval = 0 /s/0/5/" -i /etc/ganglia/gmond.conf
  sed -e "/name = \"unspecified\" /s/unspecified/${cluster_name}/" -i /etc/ganglia/gmond.conf
  sed -e '/mcast_join /s/^  /  #/' -i /etc/ganglia/gmond.conf
  sed -e '/bind /s/^  /  #/' -i /etc/ganglia/gmond.conf
  sed -e "/udp_send_channel {/a\  host = ${master_hostname}" -i /etc/ganglia/gmond.conf

  local worker_count=$(/usr/share/google/get_metadata_value dataproc-worker-count)
  if [[ "$(hostname -s)" == "${master_hostname}" ]]; then
    # Setup Ganglia host only on the master node ("0"-master in HA mode)
    setup_ganglia_host || err 'Setting up Ganglia host failed'
    if [[ "${worker_count}" == "0" ]] ; then
      # on single node instances, also configure ganglia-monitor
      setup_ganglia_worker
    fi
  else
    setup_ganglia_worker
  fi
}

main
