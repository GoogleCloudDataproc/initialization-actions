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

## Make global changes here
update_apt_get
apt-get install -y vim
update-alternatives --set editor /usr/bin/vim.basic
#apt-get install -y tmux sl

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
