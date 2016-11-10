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

# Variables for this script
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
CLUSTER_NAME=$(hostname | sed -r 's/(.*)-[w|m](-[0-9]+)?$/\1/')

# Download and extract ZooKeeper
apt-get install -y zookeeper
mkdir -p /var/lib/zookeeper

# Configure ZooKeeper node ID
NODE_NUMBER=1
if [[ "${ROLE}" == 'Worker' ]]; then
	NODE_NUMBER=$((`hostname | sed 's/.*-w-\([0-9]\)*.*/\1/g'`+2))
fi
echo ${NODE_NUMBER} > /var/lib/zookeeper/myid

# Write ZooKeeper configuration file
cat > /etc/zookeeper/conf/zoo.cfg <<EOF
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181
initLimit=5
syncLimit=2
server.1=${CLUSTER_NAME}-m:2888:3888
server.2=${CLUSTER_NAME}-w-0:2888:3888
server.3=${CLUSTER_NAME}-w-1:2888:3888
EOF

# Start ZooKeeper
ZOO_LOG_DIR=/var/log/zookeeper /usr/lib/zookeeper/bin/zkServer.sh start
