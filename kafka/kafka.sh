#!/bin/bash
#    Copyright 2015 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
set -x -e


# Only run the installation on workers; verify zookeeper on master(s)
if [[ $(/usr/share/google/get_metadata_value attributes/dataproc-role) == 'Master' ]]; then
  service zookeeper-server status || \
      (echo 'Required zookeeper-server not running on master!' && exit 1)
  # On master nodes, just install kafka libs but not kafka-server.
  apt-get install -y kafka
  exit 0
fi

# Find zookeeper list first, before attempting any installation.
ZOOKEEPER_CLIENT_PORT=$(grep clientPort /etc/zookeeper/conf/zoo.cfg | cut -d '=' -f 2)
ZOOKEEPER_LIST=$(grep "^server\." /etc/zookeeper/conf/zoo.cfg | \
        cut -d '=' -f 2 | cut -d ':' -f 1 | sed "s/$/:${ZOOKEEPER_CLIENT_PORT}/" | \
        xargs echo | sed "s/ /,/g")
if [[ -z "${ZOOKEEPER_LIST}" ]]; then
  # Didn't find zookeeper quorum in zoo.cfg, but possibly workers just didn't
  # bother to populate it. Check if YARN HA is configured.
  ZOOKEEPER_LIST=$(bdconfig get_property_value --configuration_file \
      /etc/hadoop/conf/yarn-site.xml \
      --name yarn.resourcemanager.zk-address 2>/dev/null)
fi

# If all attempts failed, error out.
if [[ -z "${ZOOKEEPER_LIST}" ]]; then
  echo 'Failed to find configured Zookeeper list; try --num-masters=3 for HA'
  exit 1
fi

# Install Kafka from Dataproc distro.
apt-get install -y kafka-server || dpkg -l kafka-server

mkdir -p /var/lib/kafka-logs
chown kafka:kafka -R /var/lib/kafka-logs

# Note: If modified to also run brokers on master nodes, this logic for
# generating BROKER_ID will need to be changed.
BROKER_ID=$(hostname | sed 's/.*-w-\([0-9]\)*.*/\1/g')
sed -i 's|log.dirs=/tmp/kafka-logs|log.dirs=/var/lib/kafka-logs|' /etc/kafka/conf/server.properties
sed -i 's|^\(zookeeper\.connect=\).*|\1'${ZOOKEEPER_LIST}'|' /etc/kafka/conf/server.properties
sed -i 's,^\(broker\.id=\).*,\1'${BROKER_ID}',' /etc/kafka/conf/server.properties
echo 'delete.topic.enable = true' >> /etc/kafka/conf/server.properties

# Start Kafka
service kafka-server restart
