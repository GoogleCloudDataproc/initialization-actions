#!/bin/bash
#    Copyright 2018,2023 Google LLC
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

distribution=$(. /etc/os-release;echo $ID$VERSION_ID)

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

readonly HBASE_HOME='/usr/lib/hbase'
mkdir -p "${HBASE_HOME}/{lib,conf,logs}"

readonly BIGTABLE_HBASE_CLIENT_1X_REPO="https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-1.x-hadoop"
readonly BIGTABLE_HBASE_CLIENT_1X_VERSION='1.29.2'
readonly BIGTABLE_HBASE_CLIENT_1X_JAR="bigtable-hbase-1.x-hadoop-${BIGTABLE_HBASE_CLIENT_1X_VERSION}.jar"
readonly BIGTABLE_HBASE_CLIENT_1X_URL="${BIGTABLE_HBASE_CLIENT_1X_REPO}/${BIGTABLE_HBASE_CLIENT_1X_VERSION}/${BIGTABLE_HBASE_CLIENT_1X_JAR}"

readonly BIGTABLE_HBASE_CLIENT_2X_REPO="https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-2.x"
readonly BIGTABLE_HBASE_CLIENT_2X_VERSION='2.7.4'
#readonly BIGTABLE_HBASE_CLIENT_2X_VERSION='2.3.6'
readonly BIGTABLE_HBASE_CLIENT_2X_JAR="bigtable-hbase-2.x-${BIGTABLE_HBASE_CLIENT_2X_VERSION}.jar"
readonly BIGTABLE_HBASE_CLIENT_2X_URL="${BIGTABLE_HBASE_CLIENT_2X_REPO}/${BIGTABLE_HBASE_CLIENT_2X_VERSION}/${BIGTABLE_HBASE_CLIENT_2X_JAR}"

readonly HBASE_VERSION="2.3.6"

readonly SCH_REPO="https://repo.hortonworks.com/content/groups/public/com/hortonworks"
readonly SHC_VERSION='1.1.1-2.1-s_2.11'
readonly SHC_JAR="shc-core-${SHC_VERSION}.jar"
readonly SHC_EXAMPLES_JAR="shc-examples-${SHC_VERSION}.jar"
readonly SHC_URL="${SCH_REPO}/shc-core/${SHC_VERSION}/${SHC_JAR}"
readonly SHC_EXAMPLES_URL="${SCH_REPO}/shc-examples/${SHC_VERSION}/${SHC_EXAMPLES_JAR}"

readonly BIGTABLE_INSTANCE="$(/usr/share/google/get_metadata_value attributes/bigtable-instance)"
if [[ -z "${BIGTABLE_INSTANCE}" ]]; then
  echo "failed to determine bigtable-instance attribute ; https://github.com/GoogleCloudDataproc/initialization-actions/tree/master/bigtable#using-this-initialization-action"
  exit 1
fi
readonly BIGTABLE_PROJECT="$(/usr/share/google/get_metadata_value attributes/bigtable-project ||
    /usr/share/google/get_metadata_value ../project/project-id)"

function retry_command() {
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
  if [[ ${DATAPROC_IMAGE_VERSION%%.*} -ge 2 ]]; then
    local -r bigtable_hbase_client_jar="$BIGTABLE_HBASE_CLIENT_2X_JAR"
    local -r bigtable_hbase_client_url="$BIGTABLE_HBASE_CLIENT_2X_URL"
  else
    local -r bigtable_hbase_client_jar="$BIGTABLE_HBASE_CLIENT_1X_JAR"
    local -r bigtable_hbase_client_url="$BIGTABLE_HBASE_CLIENT_1X_URL"
  fi

  local out="${HBASE_HOME}/lib/${bigtable_hbase_client_jar}"
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    "${bigtable_hbase_client_url}" -O "${out}"
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

function configure_bigtable_client_1x() {
  #Update classpath with shc location
  cat <<'EOF' >>/etc/spark/conf/spark-env.sh
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/spark/external/shc-core.jar"
EOF

  local -r hbase_config=$(mktemp /tmp/hbase-site.xml-XXXX)
  cat <<EOF >${hbase_config}
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

  if [[ -f "${HBASE_HOME}/conf/hbase-site.xml" ]]; then
    bdconfig merge_configurations \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --source_configuration_file "$hbase_config" \
      --clobber
  else
    cp "$hbase_config" "${HBASE_HOME}/conf/hbase-site.xml"
  fi
}

function configure_bigtable_client_2x() {
  local -r hbase_config=$(mktemp /tmp/hbase-site.xml-XXXX)
  cat <<EOF >${hbase_config}
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>google.bigtable.project.id</name><value>${BIGTABLE_PROJECT}</value></property>
  <property><name>google.bigtable.instance.id</name><value>${BIGTABLE_INSTANCE}</value></property>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase2_x.BigtableConnection</value>
  </property>
  <property>
    <name>hbase.client.registry.impl</name>
    <value>org.apache.hadoop.hbase.client.BigtableConnectionsRegistry</value>
  </property>
  <property>
    <name>hbase.client.async.connection.impl</name>
    <value>org.apache.hadoop.hbase.client.BigtableAsyncConnection</value>
  </property>
  <!-- Spark-HBase-connector uses namespaces, which bigtable doesn't support. This has the
  Bigtable client log warns rather than throw -->
  <property><name>google.bigtable.namespace.warnings</name><value>true</value></property>
</configuration>
EOF

  if [[ -f "${HBASE_HOME}/conf/hbase-site.xml" ]]; then
    bdconfig merge_configurations \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --source_configuration_file "$hbase_config" \
      --clobber
  else
    cp "$hbase_config" "${HBASE_HOME}/conf/hbase-site.xml"
  fi

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
EOF

  if [[ ${DATAPROC_IMAGE_VERSION%%.*} -ge 2 ]]; then
    configure_bigtable_client_2x || err 'Failed to configure big table 2.x client.'
  else
    configure_bigtable_client_1x || err 'Failed to configure big table 1.x client.'
  fi
}

function install_hbase() {
  if command -v apt-get >/dev/null; then
    case "${DATAPROC_IMAGE_VERSION}" in
      "1.3" | "1.4" | "1.5" | "2.0" )

        # On images prior to Dataproc 2.1, hbase can be installed from Google's bigtop repositories
        retry_command "apt-get update" || err 'Unable to update packages lists.'
        retry_command "apt-get install -y hbase" || err 'Unable to install HBase.'
        ;;

      "2.1" | "2.2" )

        # get hbase tar from official site
        # Variants:
        # * hadoop3-client-bin
        # * hadoop3-bin
        # * client-bin
        # * bin
        echo "preparing to install hbase"
        local VARIANT="bin"
        local BASENAME="hbase-${HBASE_VERSION}-${VARIANT}.tar.gz"
        echo "hbase dist basename: ${BASENAME}"
        wget "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/${BASENAME}" -P /tmp || err 'Unable to download tar'

        # extract binaries from bundle
        mkdir -p "/tmp/hbase-${HBASE_VERSION}/" "${HBASE_HOME}"
        tar xzf "/tmp/${BASENAME}" --strip-components=1 -C "/tmp/hbase-${HBASE_VERSION}/"
        cp -a "/tmp/hbase-${HBASE_VERSION}/." "${HBASE_HOME}/"

        # prune incompatible jars
        INCOMPATIBLE_JARS="guava protobuf-java"
        for pkg in ${INCOMPATIBLE_JARS} ; do
          find ${HBASE_HOME}/lib -name "$pkg-*.jar" -delete
        done

        # include jars with expected symbols
        MVN_PFX="https://repo1.maven.org/maven2"
        for url in \
          "${MVN_PFX}/com/google/cloud/bigtable/bigtable-hbase-2.x-hadoop/2.7.4/bigtable-hbase-2.x-hadoop-2.7.4.jar" \
          "${MVN_PFX}/com/google/cloud/google-cloud-bigtable/2.20.4/google-cloud-bigtable-2.20.4.jar" \
          "${MVN_PFX}/com/google/api/grpc/proto-google-cloud-bigtable-admin-v2/2.20.4/proto-google-cloud-bigtable-admin-v2-2.20.4.jar" \
          "${MVN_PFX}/com/google/protobuf/protobuf-java/3.23.0/protobuf-java-3.23.0.jar" \
          "${MVN_PFX}/net/bytebuddy/byte-buddy/1.14.8/byte-buddy-1.14.8.jar" \
          "${MVN_PFX}/org/threeten/threetenbp/1.6.8/threetenbp-1.6.8.jar"
        do
          wget -P "${HBASE_HOME}/lib" ${url}
        done

        # install hbase and hbase-config.sh into normal user $PATH
        ln -sf ${HBASE_HOME}/bin/hbase /usr/bin/
        ln -sf ${HBASE_HOME}/bin/hbase-config.sh /usr/bin/

        ;;
      "*")
        echo "unsupported DATAPROC_IMAGE_VERSION: ${DATAPROC_IMAGE_VERSION}" >&2
        exit 1
        ;;
    esac
  else
    retry_command "yum -y update" || err 'Unable to update packages lists.'
    retry_command "yum -y install hbase" || err 'Unable to install HBase.'
  fi
}

function main() {
  install_hbase             || err 'Failed to install HBase.'
  install_bigtable_client   || err 'Unable to install big table client.'
  install_shc               || err 'Failed to install Spark-HBase connector.'
  configure_bigtable_client || err 'Failed to configure big table client.'
}

main
