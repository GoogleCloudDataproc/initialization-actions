#!/bin/bash
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

# This init script installs RStudio Server on the master node of a Cloud
# Dataproc cluster.

set -x -e

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

# Only run on the master node
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
RSTUDIO_VERSION=1.1.463

if [[ "${ROLE}" == 'Master' ]]; then
  # Install RStudio Server
  update_apt_get
  apt-get install -y software-properties-common
  apt-key adv --no-tty --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF'
  add-apt-repository 'deb http://cran.r-project.org/bin/linux/debian stretch-cran34/'
  update_apt_get
  apt install -y r-base r-base-dev gdebi-core

  cd /tmp; wget https://download2.rstudio.org/rstudio-server-stretch-${RSTUDIO_VERSION}-amd64.deb
  gdebi -n /tmp/rstudio-server-stretch-${RSTUDIO_VERSION}-amd64.deb

  if ! [ $(getent group rstudio) ]; then
    groupadd rstudio
  fi
  if ! [ $(id -u rstudio) ]; then
    useradd --create-home --gid rstudio rstudio
    echo "rstudio:rstudio" | chpasswd
  fi
fi
