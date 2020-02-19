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

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

readonly HBASE_HOME='/usr/lib/hbase'

readonly BIGTABLE_HBASE_CLIENT_REPO="https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-1.x-hadoop"
readonly BIGTABLE_HBASE_CLIENT_VERSION='1.13.0'
readonly BIGTABLE_HBASE_CLIENT_JAR="bigtable-hbase-1.x-hadoop-${BIGTABLE_HBASE_CLIENT_VERSION}.jar"
readonly BIGTABLE_HBASE_CLIENT_URL="${BIGTABLE_HBASE_CLIENT_REPO}/${BIGTABLE_HBASE_CLIENT_VERSION}/${BIGTABLE_HBASE_CLIENT_JAR}"

readonly SCH_REPO="https://repo.hortonworks.com/content/groups/public/com/hortonworks"
readonly SHC_VERSION='1.1.1-2.1-s_2.11'
readonly SHC_JAR="shc-core-${SHC_VERSION}.jar"
readonly SHC_EXAMPLES_JAR="shc-examples-${SHC_VERSION}.jar"
readonly SHC_URL="${SCH_REPO}/shc-core/${SHC_VERSION}/${SHC_JAR}"
readonly SHC_EXAMPLES_URL="${SCH_REPO}/shc-examples/${SHC_VERSION}/${SHC_EXAMPLES_JAR}"

readonly BIGTABLE_INSTANCE="$(/usr/share/google/get_metadata_value attributes/bigtable-instance)"
readonly BIGTABLE_PROJECT="$(/usr/share/google/get_metadata_value attributes/bigtable-project ||
    /usr/share/google/get_metadata_value ../project/project-id)"

function retry_apt_command() {
  local -r cmd="${1}"
  for ((i = 0; i < 10; i++)); do
    if eval "${cmd}"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function install_bigtable_client() {
  local out="${HBASE_HOME}/lib/${BIGTABLE_HBASE_CLIENT_JAR}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${BIGTABLE_HBASE_CLIENT_URL}" -O "${out}"
}

function install_shc() {
  mkdir -p "/usr/lib/spark/external"
  local out="/usr/lib/spark/external/${SHC_JAR}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${SHC_URL}" -O "${out}"
  ln -s "${out}" "/usr/lib/spark/external/shc-core.jar"
  local example_out="/usr/lib/spark/examples/jars/${SHC_EXAMPLES_JAR}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${SHC_EXAMPLES_URL}" -O "${example_out}"
  ln -s "${example_out}" "/usr/lib/spark/examples/jars/shc-examples.jar"
}

function configure_bigtable_client() {
  #Update classpaths
  cat <<'EOF' >>/etc/hadoop/conf/mapred-env.sh
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/usr/lib/hbase/*"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/usr/lib/hbase/lib/*"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/etc/hbase/conf"
EOF
  cat <<'EOF' >>/etc/spark/conf/spark-env.sh
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/hbase/*"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/hbase/lib/*"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/etc/hbase/conf"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/spark/external/shc-core.jar"
EOF

  cat <<EOF >hbase-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>google.bigtable.project.id</name><value>${BIGTABLE_PROJECT}</value></property>
  <property><name>google.bigtable.instance.id</name><value>${BIGTABLE_INSTANCE}</value></property>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase1_x.BigtableConnection</value>
  </property>
  <!-- Spark-HBase-connector uses namespaces, which bigtable doesn't support. This has the
  Bigtable client log warns rather than throw -->
  <property><name>google.bigtable.namespace.warnings</name><value>true</value></property>
</configuration>
EOF

  bdconfig merge_configurations \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --source_configuration_file hbase-site.xml \
    --clobber
}

function main() {
  retry_apt_command "apt-get update" || err 'Unable to update packages lists.'
  retry_apt_command "apt-get install -y hbase" || err 'Unable to install HBase.'

  install_bigtable_client || err 'Unable to install big table client.'
  configure_bigtable_client || err 'Failed to configure big table client.'

  install_shc || err 'Failed to install Spark-HBase connector.'
}

main
