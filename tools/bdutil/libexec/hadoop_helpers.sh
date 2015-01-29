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

# Helper functions to start the namenode and jobtracker and then wait for
# the namenode and jobtracker to begin running.

MAX_ATTEMPTS=15
# Repeatedly check for namenode status until running.
function wait_for_namenode() {
  if [ ! ${ENABLE_HDFS} ]; then
    return 0
  fi
  local get_namenode="hadoop dfsadmin -Dfs.default.name=${NAMENODE_URI} -report"
  local sleep_time=${BDUTIL_POLL_INTERVAL_SECONDS}
  for (( i=0; i < ${MAX_ATTEMPTS}; i++ )); do
    if ${get_namenode}; then
      return 0
    else
      # Save the error code responsible for the trap.
      local errcode=$?
      echo "Namenode not yet ready (${errcode}); sleeping ${sleep_time}." >&2
      sleep ${sleep_time}
    fi
  done
  echo "Namenode not ready after ${max_attempts} attempts" >&2
  return ${errcode}
}

# Repeatedly check for jobtracker status until running.
function wait_for_jobtracker() {
  local sleep_time=${BDUTIL_POLL_INTERVAL_SECONDS}
  for (( i=0; i < ${MAX_ATTEMPTS}; i++ )); do
    local jobtracker_running=$(grep -c "JobTracker: Starting RUNNING" \
        /hadoop/logs/hadoop-hadoop-jobtracker-${MASTER_HOSTNAME}.log)
    if [ "${jobtracker_running}" -gt "0" ]; then
      return 0
    else
      # Save the error code responsible for the trap.
      local errcode=$?
      echo "Jobtracker not yet ready(${errcode}); sleeping ${sleep_time}." >&2
      sleep ${sleep_time}
    fi
  done
  echo "Jobtracker not ready after ${max_attempts} attempts" >&2
  return ${errcode}
}

# Start dfs for hadoop1.
# Use ./bin/start-dfs.sh to start the namenode.
function start_dfs_hadoop_1() {
  if [ -z "$(find /mnt/*/hadoop/ /hadoop/ -maxdepth 4 \
      -wholename '*/name/current/VERSION')" ]; then
    # namenode is not formatted
    sudo -u hadoop ./bin/hadoop namenode -format
  fi
  sudo -u hadoop ./bin/start-dfs.sh
}

# Start dfs for hadoop2.
# Use ./sbin/start-dfs.sh to start the namenode.
function start_dfs_hadoop_2() {
  if [ -z "$(find /mnt/*/hadoop/ /hadoop/ -maxdepth 4 \
      -wholename '*/name/current/VERSION')" ]; then
    # namenode is not formatted
    sudo -u hadoop ./bin/hadoop namenode -format
  fi
  sudo -u hadoop ./sbin/start-dfs.sh
}

# Start up job and task trackers
function start_mapred() {
  sudo -u hadoop ./bin/start-mapred.sh
}


# Validate namenode is running.  First parameter is a function
# that calls to start_dfs.sh
function start_with_retry_namenode() {
  local start_dfs_file=$1
  ${start_dfs_file}
  local max_attempts=3
  for (( i=0; i < ${max_attempts}; i++ )); do
    if wait_for_namenode; then
      return 0
    else
      # Save the error code responsible for the trap.
      local errcode=$?
      echo "Namenode not yet running (${errcode}); Retrying start-dfs.sh" >&2
      ${start_dfs_file}
    fi
  done
  if !(wait_for_namenode); then
    echo "Namenode not running after ${max_attempts} attempts at start-dfs.sh" \
        >&2
    return ${errcode}
  fi
}

# Validate jobtracker is running.
function start_with_retry_jobtracker() {
  start_mapred
  local max_attempts=3
  for (( i=0; i < ${max_attempts}; i++ )); do
    if wait_for_jobtracker; then
      return 0
    else
      # Save the error code responsible for the trap.
      local errcode=$?
      echo "JobTracker not yet running (${errcode}); Retrying start-mapred.sh" \
          >&2
      start_mapred
    fi
  done
  if wait_for_jobtracker; then
    return 0
  else
    local errcode=$?
    echo "JobTracker not running after ${max_attempts} attempts at \
        start-mapred.sh" >&2
    return ${errcode}
  fi
}

# Check filesystem accessibility, diagnosing further in the case of GCS to
# differentiate between all GCS access being broken from this machine, or
# just the Hadoop GCS-connector portion.
# TODO: Also check HDFS and any other filesystem we expect to work.
function check_filesystem_accessibility() {
  if (( ${INSTALL_GCS_CONNECTOR} )) ; then
    local fs_cmd="${HADOOP_INSTALL_DIR}/bin/hadoop fs"
    if ${fs_cmd} -test -d gs://${CONFIGBUCKET}; then
      return 0
    else
      local errcode=$?
      if ! gsutil ls -b gs://${CONFIGBUCKET}; then
        echo "Error: Could not access GCS on VM $(hostname)" >&2
      else
        cat << EOF >&2
Error: There was a problem accessing your configuration bucket using the GCS
connector. Check configuration files. Also make sure have the GCS JSON API
enabled as described at https://developers.google.com/storage/docs/json_api/.
EOF
      fi
      return ${errcode}
    fi
  fi
}

# Determine the HDFS superuser
function get_hdfs_superuser() {
  local hdfs_superuser=''
  if (id -u hdfs >& /dev/null); then
    hdfs_superuser='hdfs'
  elif (id -u hadoop >& /dev/null); then
    hdfs_superuser='hadoop'
  else
    logerror 'Cannot find HDFS superuser.'
    exit 1
  fi
  echo ${hdfs_superuser}
}

# Create and configure Hadoop2 specific HDFS directories.
function initialize_hdfs_dirs() {
  local hdfs_superuser=$(get_hdfs_superuser)
  local dfs_cmd="sudo -i -u ${hdfs_superuser} hadoop fs"
  loginfo "Setting up HDFS /tmp directories."
  if ! ${dfs_cmd} -stat /tmp ; then
    ${dfs_cmd} -mkdir -p /tmp/hadoop-yarn/history
    ${dfs_cmd} -mkdir -p /tmp/hadoop-yarn/staging
  fi
  ${dfs_cmd} -chmod -R 1777 /tmp

  loginfo "Setting up HDFS /user directories."
  for USER in $(getent passwd | grep '/home' | cut -d ':' -f 1); do
    if ! ${dfs_cmd} -stat /user/${USER}; then
      loginfo "Creating HDFS directory for user '${USER}'"
      ${dfs_cmd} -mkdir -p "/user/${USER}"
      ${dfs_cmd} -chown "${USER}" "/user/${USER}"
    fi
  done
}

