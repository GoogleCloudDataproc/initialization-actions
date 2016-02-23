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
#    limitations under the License.set -x -e
set -x -e


# Only run this on worker nodes
if [[ $(/usr/share/google/get_metadata_value attributes/dataproc-role) == 'Master' ]]; then
	exit 0
fi

# Variables for this script
SCALA_VERSION="2.11"
KAFKA_VERSION="0.9.0.0"
CLUSTER_NAME=$(hostname | sed 's/\(.*\)-[m|w]-[0-9]*.*/\1/g')
BROKER_ID=$(hostname | sed 's/.*-w-\([0-9]\)*.*/\1/g')
DNS_NAME=$(dnsdomainname)

# Download and extract Kafka
cd ~
wget http://www.us.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
tar zxvf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Configure Kafka
ZOOKEEPER_INSTANCES=
for h in "m" "w-0" "w-1"; do
	ZOOKEEPER_INSTANCES+=${CLUSTER_NAME}-$h.${DNS_NAME}:2181,
done
sed -i 's|^\(zookeeper\.connect=\).*|\1'${ZOOKEEPER_INSTANCES}'|' kafka_${SCALA_VERSION}-${KAFKA_VERSION}/config/server.properties
sed -i 's,^\(broker\.id=\).*,\1'${BROKER_ID}',' kafka_${SCALA_VERSION}-${KAFKA_VERSION}/config/server.properties
echo 'delete.topic.enable = true' >> kafka_${SCALA_VERSION}-${KAFKA_VERSION}/config/server.properties

# Start Kafka
nohup kafka_${SCALA_VERSION}-${KAFKA_VERSION}/bin/kafka-server-start.sh kafka_${SCALA_VERSION}-${KAFKA_VERSION}/config/server.properties > /var/log/kafka.log 2>&1 &