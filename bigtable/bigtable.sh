#!/bin/bash
#    Copyright 2018 Google, Inc.
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
#
# This actions installs cloud-bigtable-client (https://github.com/GoogleCloudPlatform/cloud-bigtable-client/)
# on dataproc cluster and configure it to use cloud BigTable (https://cloud.google.com/bigtable/).

set -euxo pipefail

readonly HBASE_HOME='/usr/lib/hbase'
readonly BIGTABLE_HBASE_CLIENT='bigtable-hbase-1.x-hadoop-1.3.0.jar'
readonly HBASE_BIGTABLE_DL_LINK="http://central.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-1.x-hadoop/1.3.0/${BIGTABLE_HBASE_CLIENT}"

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_big_table_client() {
    wget -q "${HBASE_BIGTABLE_DL_LINK}" -O "${HBASE_HOME}/lib/${BIGTABLE_HBASE_CLIENT}" \
      || err 'Unable to install BigTable client libs.'
    ln -s "${HBASE_HOME}/lib/${BIGTABLE_HBASE_CLIENT}" \
      "/usr/lib/hadoop-mapreduce/lib/${BIGTABLE_HBASE_CLIENT}"
}

function configure_big_table_client() {

#Update classpaths
  cat << 'EOF' >> /etc/hadoop/conf/mapred-env.sh
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/usr/lib/hbase/*"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/usr/lib/hbase/lib/*"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/etc/hbase/conf"
EOF
  cat << 'EOF' >> /etc/spark/conf/spark-env.sh
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/hbase/*"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/hbase/lib/*"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/etc/hbase/conf"
EOF

  cat << EOF > hbase-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>google.bigtable.project.id</name><value>${big_table_project}</value></property>
  <property><name>google.bigtable.instance.id</name><value>${big_table_instance}</value></property>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase1_x.BigtableConnection</value>
  </property>
</configuration>
EOF

  bdconfig merge_configurations \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --source_configuration_file hbase-site.xml \
    --clobber
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  big_table_instance="$(/usr/share/google/get_metadata_value attributes/bigtable-instance)"
  big_table_project="$(/usr/share/google/get_metadata_value attributes/bigtable-project || \ 
    /usr/share/google/get_metadata_value ../project/project-id)"

  update_apt_get || err 'Unable to update packages lists.'
  apt-get install -y hbase || err 'Unable to install hbase.'

  install_big_table_client || err 'Unable to install big table client'.
  configure_big_table_client || err 'Failed to configure big table client.'

}

main