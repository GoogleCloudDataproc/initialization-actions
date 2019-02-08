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

# Helper to run any command with Fibonacci backoff.
# If all retries fail, returns last attempt's exit code.
# Args: "$@" is the command to run.
function run_with_retries() {
  local retry_backoff=(1 1 2 3 5 8 13 21 34 55 89 144)
  local -a cmd=("$@")
  echo "About to run '${cmd[*]}' with retries..."

  local succeeded=0
  for ((i = 0; i < ${#retry_backoff[@]}; i++)); do
    if "${cmd[@]}"; then
      succeeded=1
      break
    else
      local sleep_time=${retry_backoff[$i]}
      echo "'${cmd[*]}' attempt $(( $i + 1 )) failed! Sleeping ${sleep_time}." >&2
      sleep ${sleep_time}
    fi
  done

  if ! (( ${succeeded} )); then
    echo "Final attempt of '${cmd[*]}'..."
    # Let any final error propagate all the way out to any error traps.
    "${cmd[@]}"
  fi
}

# Only run on the master node
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
RSTUDIO_VERSION=1.1.463

if [[ "${ROLE}" == 'Master' ]]; then
  # Install RStudio Server
  update_apt_get
  apt-get install -y software-properties-common
  run_with_retries apt-key adv --no-tty --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF'
  add-apt-repository 'deb http://cran.r-project.org/bin/linux/debian stretch-cran34/'
  update_apt_get
  apt install -y r-base r-base-dev gdebi-core

  cd /tmp; wget https://download2.rstudio.org/rstudio-server-${RSTUDIO_VERSION}-amd64.deb
  gdebi -n /tmp/rstudio-server-${RSTUDIO_VERSION}-amd64.deb

  groupadd rstudio
  useradd --create-home --gid rstudio rstudio
  echo "rstudio:rstudio" | chpasswd
fi
