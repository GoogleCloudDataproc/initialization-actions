# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Helper functions for use in other libexec/ core setup files

# Allow overriding the date function for unit testing.
function bdutil_date() {
  date "$@"
}

# Simple wrapper around "echo" so that it's easy to add log messages with a
# date/time prefix.
function loginfo() {
  echo "$(bdutil_date): ${@}"
}

# Simple wrapper around "echo" controllable with ${VERBOSE_MODE}.
function logdebug() {
  if (( ${VERBOSE_MODE} )); then
    loginfo ${@}
  fi
}

# Simple wrapper to pass errors to stderr.
function logerror() {
  loginfo ${@} >&2
}

# Helper to overwrite the contents of file specified by $1 with strings
# specified as ${@:2}, using "echo". This helper is convenient for cases
# where a command string may be passed around in a way that makes precedence
# of stdin/stdout redirection characters unclear.
# Example: overwrite_file_with_strings foo.txt hello world
function overwrite_file_with_strings() {
  local contents=${@:2}
  local filename=$1
  echo "Overwriting ${filename} with contents '${contents}'"
  echo "${contents}" > ${filename}
}

# Helper to run any command with default bdutil settings for sleep time between
# retries, max retries, etc. If all retries fail, returns last attempt's exit
# code.
# Args: "$@" is the command to run.
function run_with_retries() {
  local cmd="$@"
  echo "About to run '${cmd}' with retries..."

  local update_succeeded=0
  local sleep_time=${BDUTIL_POLL_INTERVAL_SECONDS}
  local max_attempts=5
  for ((i = 1; i < ${max_attempts}; i++)); do
    if ${cmd}; then
      update_succeeded=1
      break
    else
      echo "'${cmd}' attempt $i failed! Sleeping ${sleep_time}." >&2
      sleep ${sleep_time}
    fi
  done

  if ! (( ${update_succeeded} )); then
    echo "Final attempt of '${cmd}'..."
    # Let any final error propagate all the way out to any error traps.
    ${cmd}
  fi
}

# Attempts to curl a passed file 3 times, waiting one second between each try.
# If all curls fail, it allows the error to percolate.
function curl_with_retry() {
  UPDATE_SUCCEEDED=0
  for ((i = 1; i <= 3; i++)); do
    if curl -f "$@"; then
      UPDATE_SUCCEEDED=1
      break
    else
      sleep 1
      echo "curl attempt $i failed! Trying again..." >&2
    fi
  done

  if ! (( ${UPDATE_SUCCEEDED} )); then
    echo "curl failed to get file " $1 " 3 times" >&2
    exit 1
  fi
}

function download_bd_resource() {
  local remote_uri=$1
  local local_destination=$2
  if [[ -z "${local_destination}" ]]; then
    local_destination=$(basename ${remote_uri})
  fi

  if [[ $remote_uri == gs://* ]]; then
    if (( ${DEBUG_MODE} )); then
      gsutil -D cp "${remote_uri}" "${local_destination}"
    else
      gsutil cp "${remote_uri}" "${local_destination}"
    fi
  else
    curl_with_retry "${remote_uri}" -o "${local_destination}"
  fi
}

readonly APT_SENTINEL='apt.lastupdate'

function update_apt_get() {
  local update_succeeded=0
  local sleep_time=${BDUTIL_POLL_INTERVAL_SECONDS}
  local max_attempts=5
  for ((i = 1; i <= ${max_attempts}; i++)); do
    if apt-get -y -qq update; then
      update_succeeded=1
      break
    else
      echo "apt-get update attempt $i failed! Sleeping ${sleep_time}." >&2
      sleep ${sleep_time}
    fi
  done

  if ! (( ${update_succeeded} )); then
    echo 'Final attempt to apt-get update...'
    # Let any final error propagate all the way out to any error traps.
    apt-get -y -qq update
  fi
  touch "${APT_SENTINEL}"
}

function update_apt_get_if_needed() {
  # If the apt sentinel doesn't exist or if it hasn't ben touched in the
  # last hour, update apt
  if [[ "$(find $(dirname ${APT_SENTINEL}) -maxdepth 1 \
      -name $(basename ${APT_SENTINEL}) -newermt '-1 hour')" == "" ]]; then
    update_apt_get
  fi
}

# Attempts to apt-get update and install a passed apt_get_package_name name
# If it can't find apt-get it will try to use yum
function install_application() {
  local apt_get_package_name=$1
  local yum_package_name=$1

  NUMBER_OF_ARGS=${#@}
  if (( $NUMBER_OF_ARGS == 2 )); then
    yum_package_name=$2
  fi

  if [[ -x $(which apt-get) ]]; then
    if $(dpkg -s ${apt_get_package_name}); then
      echo "${apt_get_package_name} already installed."
    else
      if (( ${STRIP_EXTERNAL_MIRRORS} )); then
        echo 'Stripping external apt-get mirrors...'
        sed -i "s/.*security.debian.org.*//" /etc/apt/sources.list
        sed -i "s/.*http.debian.net.*//" /etc/apt/sources.list
      else
        echo "Using default apt-get mirror list for ${apt_get_package_name} installation..."
      fi

      update_apt_get_if_needed

      apt-get -y -qq install ${apt_get_package_name}
    fi
  elif [[ -x $(which yum) ]]; then
    if rpm -q ${yum_package_name}; then
      echo "${yum_package_name} already installed."
    else
      yum -y -q install ${yum_package_name}
    fi
  else
    echo "Cannot find package manager to install ${apt_get_package_name}. Exiting" >&2
    exit 1
  fi
}

# True if the specified single IPv4 tcp port is available.
function tcp_port_is_free() {
  port=$1
  ss_lines=$(ss -a -t "( sport = :${port} )" | wc -l)
  return $(( ss_lines > 1 ))
}

# True if all specified IPv4 tcp ports are available.
function all_tcp_ports_are_free() {
  ports="$@"
  for p in $ports; do
    if ! tcp_port_is_free $p; then
      return 1
    fi
  done
  return 0
}

# Prints the list of ports in use.
function tcp_ports_in_use() {
  ports="$@"
  local in_use=()
  for p in $ports; do
    if ! tcp_port_is_free $p; then
      in_use+=($p)
    fi
  done
  echo ${in_use[@]}
}

# Wait until all of the specified IPv4 tcp ports are unused.
# Returns 0 (true) if all ports are free, or 1 (false) if we timed out
# before all ports are free. The timeout can be set by specifying it
# with the --max_wait_seconds option (must be the first argument).
# All other arguments are port names or numbers.
function wait_until_ports_free() {
  local start_time=$(date '+%s')
  local now=${start_time}
  local max_wait_seconds=30
  local seconds_between_checks=1
  if [[ "$1" == '--max_wait_seconds' ]]; then
    max_wait_seconds=$2
    shift 2
  fi
  ports="$@"
  while (( now < start_time + max_wait_seconds )); do
    if all_tcp_ports_are_free $ports; then
      return 0
    fi
    sleep $seconds_between_checks
    now=$(date '+%s')
  done
  return 1    # Timed out before all ports are free.
}

# Wait for ports to be free, and output some info about it.
# $1 is an array of port numbers.
function wait_until_ports_free_and_report() {
  PORTS=("$@")
  echo "Testing for available ports: ${PORTS[*]}"
  local in_use=$(tcp_ports_in_use "${PORTS[@]}")
  if [[ "${in_use}" == "" ]]; then
    echo "All tested ports are available."
    return 0
  fi
  echo "Waiting for ports that are not yet available: ${in_use}."
  local port_wait_start_time=$(date '+%s')
  local port_wait_status=0
  wait_until_ports_free ${PORTS[@]} || port_wait_status=$?
  local port_wait_end_time=$(date '+%s')
  local port_wait_time=$(( port_wait_end_time - port_wait_start_time ))
  if [[ ${port_wait_time} -gt 0 ]]; then
    echo "Wait time in seconds for ports ${PORTS[@]} was ${port_wait_time}."
    local still_in_use=$(tcp_ports_in_use ${PORTS[@]})
    if [[ "${still_in_use}" != "" ]]; then
      echo "Ports still in use: ${still_in_use}."
    fi
  fi
  if [[ "${still_in_use}" == "" ]]; then
    echo "All tested ports are now available."
  fi
  return ${port_wait_status}
}

# Instead of wiring a variable through bdutil_env, we will
# invoke this function to get the location on the master
# that the GCS file cache should be mounted to. This will be
# used in both setup_master_nfs and setup_worker_nfs.
function get_nfs_mount_point() {
  echo "/hadoop_gcs_connector_metadata_cache"
}

# Instead of wiring a variable through bdutil_env, we will
# invoke this function to get the location on the master
# that the GCS file cache is exported from. This will be
# used in both setup_master_nfs and setup_worker_nfs.
function get_nfs_export_point() {
  echo "/export/hadoop_gcs_connector_metadata_cache"
}

# Checks whether user GCS_ADMIN exists already, and if not,
# adds the user and creates a home directory for it.
function setup_gcs_admin() {
  if [ -z "${GCS_ADMIN}" ] ; then
    echo "ERROR: No GCS_ADMIN defined."
    exit 1
  fi

  if ! (id -u ${GCS_ADMIN} >& /dev/null); then
    mkdir -p /home/${GCS_ADMIN}
    useradd --system --shell /usr/sbin/nologin -M \
        --home /home/${GCS_ADMIN} --user-group ${GCS_ADMIN}
    chown -R ${GCS_ADMIN}:${GCS_ADMIN} /home/${GCS_ADMIN}
  fi
}

# Prints a script which can be run to perform garbage-collection of GCS cache
# entries.
function make_cache_cleaner_script() {
  # Class name of our cache cleaner
  local gc_cleaner='com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemCacheCleaner'

  # NFS export table:
  local etab='/var/lib/nfs/etab'

  cat <<EOF
#!/usr/bin/env bash

export HADOOP_LOGFILE='gcs-cache-cleaner.log'
export HADOOP_ROOT_LOGGER='${GCS_CACHE_CLEANER_LOGGER}'

${HADOOP_INSTALL_DIR}/bin/hadoop ${gc_cleaner} > \
    ${GCS_CACHE_CLEANER_LOG_DIRECTORY}/gcs-cache-cleaner.out
EOF
}

# Given full path to a cache-cleaner script as $1, prints the crontab entry
# for running it as user ${GCS_CACHE_CLEANER_USER} twice an hour; caller can
# append the printed contents into an actual crontab to activate it as needed.
function make_cleaner_crontab() {
  local cleaner=$1

  cat <<EOF
# Run the ${cleaner} script twice every hour at 7 and 37 minutes past the hour
# m h dom mon dow user command
7,37 * * * * ${GCS_CACHE_CLEANER_USER} ${cleaner}
EOF
}

# Sets up a directory and adds a cache-cleaner script, then configures a
# crontab to periodically clean the gcs cache automatically.
function setup_cache_cleaner() {
  # Create the cleaner script in a subdirectory of HADOOP_INSTALL_DIR.
  mkdir -p "${HADOOP_INSTALL_DIR}/google"
  local cleaner="${HADOOP_INSTALL_DIR}/google/clean-caches.sh"
  make_cache_cleaner_script > "${cleaner}"

  # Create the crontab for running the script.
  chmod 755 "${cleaner}"
  make_cleaner_crontab "${cleaner}" > "/etc/cron.d/clean-gcs-caches"
}

# Adds text to login scripts. Text is passed via STDIN.
function add_to_login_scripts() {
  tee -a /etc/profile.d/bdutil_env.sh /etc/*bashrc > /dev/null
}

# Adds directory to all users "$PATH"s. Takes the directory to add.
function add_to_path_at_login() {
  local directory="$1"
  if [[ ! -d ${directory} ]]; then
    echo "Error: Cannot find directory: ${directory}" >&2
    return 1
  fi
  cat << EOF | add_to_login_scripts
if [[ -d ${directory} ]]; then
  export PATH=\$PATH:${directory}
fi
EOF
}
