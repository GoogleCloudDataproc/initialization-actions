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

# Variables for this script
HBASE_VERSION="0.98.18"
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
CLUSTER_NAME=$(hostname | sed -r 's/(.*)-[w|m](-[0-9]+)?$/\1/')

# Download and extract ZooKeeper
cd ~
wget http://mirrors.advancedhosters.com/apache/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-hadoop2-bin.tar.gz
tar zvxf hbase-${HBASE_VERSION}-hadoop2-bin.tar.gz
cd ~/hbase-${HBASE_VERSION}-hadoop2
rm -rf hbase-${HBASE_VERSION}-hadoop2-bin.tar.gz

cat > conf/hbase-site-patch.xml <<EOF
  <property>
  <name>hbase.zookeeper.quorum</name>
    <value>${CLUSTER_NAME}-m,${CLUSTER_NAME}-w-0,${CLUSTER_NAME}-w-1</value>
  </property>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://${CLUSTER_NAME}-m/hbase</value>
  </property>
EOF

sed -i '/<\/configuration>/e cat hbase-site-patch.xml' \
     conf/hbase-site.xml
rm -rf conf/hbase-site-patch.xml

if [[ "${ROLE}" == 'Master' ]]; then
  bin/hbase-daemon.sh start master
else
  bin/hbase-daemon.sh start regionserver
fi
