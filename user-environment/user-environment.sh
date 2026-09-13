#!/bin/bash
# Copyright 2015 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o xtrace

function os_id()       ( set +x ;  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_version()  ( set +x ;  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs ; )
function os_codename() ( set +x ;  grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs ; )

function version_ge() ( set +x ;  [ "$1" = "$(echo -e "$1\n$2" | sort -V | tail -n1)" ] ; )
function version_gt() ( set +x ;  [ "$1" = "$2" ] && return 1 || version_ge $1 $2 ; )
function version_le() ( set +x ;  [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ] ; )
function version_lt() ( set +x ;  [ "$1" = "$2" ] && return 1 || version_le $1 $2 ; )

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

function is_debuntu()  ( set +x ;  is_debian || is_ubuntu ; )

readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

function repair_old_backports {
  if ! is_debuntu ; then return ; fi
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  debdists="https://deb.debian.org/debian/dists"
  oldoldstable=$(curl -s "${debdists}/oldoldstable/Release" | awk '/^Codename/ {print $2}');
  oldstable=$(   curl -s "${debdists}/oldstable/Release"    | awk '/^Codename/ {print $2}');
  stable=$(      curl -s "${debdists}/stable/Release"       | awk '/^Codename/ {print $2}');

  matched_files=( $(grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  for filename in "${matched_files[@]}"; do
    perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                  {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
  done
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

## Only install customize master node by default.
## Delete to customize all nodes.
[[ "${HOSTNAME}" =~ -m$ ]] || exit 0

if [[ ${OS_NAME} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
  repair_old_backports
fi

## Make global changes here

if is_debuntu ; then 
  update_apt_get
  apt-get install -y vim
  update-alternatives --set editor /usr/bin/vim.basic
  #apt-get install -y tmux sl
fi

## The following script will get run as each user in their home directory.
cat <<'EOF' >/tmp/customize_home_dir.sh
set -o errexit
set -o nounset
set -o xtrace

## Uncomment all options in Debian's .bashrc
## Includes a forced color prompt, ls aliases, misc.
sed -Ei 's/#(\S)/\1/' ${HOME}/.bashrc

## Optionally customize .bashrc further
cat << 'EOT' >> ${HOME}/.bashrc
## Useful aliases
#alias rm='rm -i'
#alias cp='cp -i'
#alias mv='mv -i'
#alias sl='sl -e'
#alias LS='LS -e'

## Add ISO 8601 formatted date time to prompt.
#PS1="\[\033[01;34m\][\D{%FT%T}]\[\033[00m\] ${PS1}"
EOT

## Make any other changes here.
## e.g. use the system provided .vimrc
#cp /usr/share/vim/vimrc ${HOME}/.vimrc
## Uncomment optional settings
#sed -Ei 's/^"(\S|  )/\1/' ${HOME}/.vimrc
## Except compatible, because no one wants that.
#sed -i 's/set compatible/"&/' ${HOME}/.vimrc
EOF

## Use members of the group adm as the list of users worth customizing.
USERS="$(getent group adm | sed -e 's/.*://' -e 's/,/ /g')"
for user in ${USERS}; do
  sudo -iu ${user} bash /tmp/customize_home_dir.sh
done

## Customize /etc/skel for future users.
HOME=/etc/skel bash /tmp/customize_home_dir.sh
