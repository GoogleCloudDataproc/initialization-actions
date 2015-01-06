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

# Starts relevant hadoop daemon servers as the 'hadoop' user.

set -e

source hadoop_helpers.sh

HADOOP_PORTS=(8088 50010 50020 50070 50090)

cd ${HADOOP_INSTALL_DIR}

# Test for sshability to workers.
for NODE in ${WORKERS[@]}; do
  sudo -u hadoop ssh ${NODE} "exit 0"
done

# Wait for our ports to be free, but keep running even if not.
wait_until_ports_free_and_report "${HADOOP_PORTS[@]}" || true

if (( ${ENABLE_HDFS} )); then
  # Start namenode and jobtracker
  start_with_retry_namenode start_dfs_hadoop_2

  # Post DFS startup:
  if ! hadoop fs -stat /tmp ; then
    sudo -u hadoop ./bin/hadoop fs -mkdir -p /tmp/hadoop-yarn/history
    sudo -u hadoop ./bin/hadoop fs -mkdir -p /tmp/hadoop-yarn/staging
    sudo -u hadoop ./bin/hadoop fs -chmod -R 1777 /tmp
  fi
  # TODO: Replace the following line with logic from start_hadoop.sh
  # that waits for DFS startup.
  sleep 60

  # Post DFS startup:
  if ! hadoop fs -stat /tmp ; then
    sudo -u hadoop ./bin/hadoop fs -mkdir -p /tmp/hadoop-yarn/history
    sudo -u hadoop ./bin/hadoop fs -mkdir -p /tmp/hadoop-yarn/staging
    sudo -u hadoop ./bin/hadoop fs -chmod -R 1777 /tmp
  fi
fi

# Start up resource and node managers
sudo -u hadoop ./sbin/start-yarn.sh
service hadoop-mapreduce-historyserver start

check_filesystem_accessibility
