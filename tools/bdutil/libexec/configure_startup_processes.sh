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

# Populates /etc/init.d scripts to keep processes up on startup

set -e

# Associative array for looking about Hadoop 2 daemon scripts
declare -r -A HADOOP_2_DAEMON_SCRIPTS=(
    [hdfs]="hadoop-daemon.sh --script ${HADOOP_INSTALL_DIR}/bin/hdfs"
    [mapreduce]="mr-jobhistory-daemon.sh"
    [yarn]="yarn-daemon.sh"
)

HADOOP_DAEMONS=()
HADOOP_VERSION=$(${HADOOP_INSTALL_DIR}/bin/hadoop version \
    | tr -cd [:digit:] | head -c1)

if [[ "$(hostname -s)" == "${MASTER_HOSTNAME}" ]]; then
  if (( ${ENABLE_HDFS} )); then
    HADOOP_DAEMONS+=('hdfs-namenode' 'hdfs-secondarynamenode')
  fi
  if (( ${HADOOP_VERSION} < 2 )); then
    HADOOP_DAEMONS+=('mapreduce-jobtracker')
  else
    HADOOP_DAEMONS+=('yarn-resourcemanager' 'mapreduce-historyserver')
  fi
else
  if (( ${ENABLE_HDFS} )); then
    HADOOP_DAEMONS+=('hdfs-datanode')
  fi
  if (( ${HADOOP_VERSION} < 2 )); then
    HADOOP_DAEMONS+=('mapreduce-tasktracker')
  else
    HADOOP_DAEMONS+=('yarn-nodemanager')
  fi
fi

for DAEMON in "${HADOOP_DAEMONS[@]}"; do
  # Split DAEMON into SERVICE and TARGET e.g. DAEMON "hdfs-namenode" splits into
  # SERVICE "hdfs" and TARGET "namenode".
  SERVICE="${DAEMON%%-*}"
  TARGET="${DAEMON#*-}"
  if (( ${HADOOP_VERSION} < 2 )); then
    DAEMON_SCRIPT="${HADOOP_INSTALL_DIR}/bin/hadoop-daemon.sh"
  else
    DAEMON_SCRIPT="${HADOOP_INSTALL_DIR}/sbin/"
    DAEMON_SCRIPT+="${HADOOP_2_DAEMON_SCRIPTS[${SERVICE}]}"
  fi

  INIT_SCRIPT=/etc/init.d/hadoop-${DAEMON}
  cat << EOF > ${INIT_SCRIPT}
#!/usr/bin/env bash
# Boot script for Hadoop ${SERVICE^^} ${TARGET^}
### BEGIN INIT INFO
# Provides:          hadoop-${DAEMON}
# Required-Start:    \$all
# Required-Stop:     \$all
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Hadoop ${SERVICE^^} ${TARGET^}
# Description:       Hadoop ${SERVICE^^} ${TARGET^} (http://hadoop.apache.org/)
### END INIT INFO

function print_usage() {
  echo "Usage: \$0 start|stop|restart" >&2
}

# Check for root
if (( \${EUID} != 0 )); then
  echo "This must be run as root." >& 2
  exit 1
fi

if (( \$# != 1 )); then
  print_usage
  exit 1
fi

case "\$1" in
  start|stop)
    su hadoop -c "${DAEMON_SCRIPT} \$1 ${TARGET}"
    RETVAL=\$?
    ;;
  restart)
    \$0 stop
    \$0 start
    RETVAL=\$?
    ;;
  *)
    print_usage
    exit 1
    ;;
esac

exit \${RETVAL}
EOF
  chmod 755 ${INIT_SCRIPT}
  if which insserv; then
    insserv ${INIT_SCRIPT}
  elif which chkconfig; then
    chkconfig --add hadoop-${DAEMON}
  else
    echo "No boot process configuration tool found." >&2
    exit 1
  fi
done
