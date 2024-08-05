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

readonly OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
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

readonly BIGTABLE_HBASE_CLIENT_2X_REPO="https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-2.x-hadoop"
readonly BIGTABLE_HBASE_CLIENT_2X_VERSION='2.12.0'
readonly BIGTABLE_HBASE_CLIENT_2X_JAR="bigtable-hbase-2.x-hadoop-${BIGTABLE_HBASE_CLIENT_2X_VERSION}.jar"
readonly BIGTABLE_HBASE_CLIENT_2X_URL="${BIGTABLE_HBASE_CLIENT_2X_REPO}/${BIGTABLE_HBASE_CLIENT_2X_VERSION}/${BIGTABLE_HBASE_CLIENT_2X_JAR}"

readonly region="$(/usr/share/google/get_metadata_value attributes/dataproc-region)"
readonly SCH_REPO="gs://dataproc-initialization-actions-${region}/jars/bigtable"
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

function remove_old_backports {
  if is_debian12 ; then return ; fi
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will use archive.debian.org for the oldoldstable repo

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  debdists="https://deb.debian.org/debian/dists"
  oldoldstable=$(curl -s "${debdists}/oldoldstable/Release" | awk '/^Codename/ {print $2}');
  oldstable=$(   curl -s "${debdists}/oldstable/Release"    | awk '/^Codename/ {print $2}');
  stable=$(      curl -s "${debdists}/stable/Release"       | awk '/^Codename/ {print $2}');

  matched_files=( $(test -d /etc/apt && grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  for filename in "${matched_files[@]}"; do
    # Fetch from archive.debian.org for ${oldoldstable}-backports
    perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                  {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
  done
}

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
  if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.0" | bc -l) == 1 ]]; then
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
  gsutil cp -r "${SHC_URL}" "${out}"
  ln -s "${out}" "/usr/lib/spark/external/shc-core.jar"
  local example_out="/usr/lib/spark/examples/jars/${SHC_EXAMPLES_JAR}"
  gsutil cp -r "${SHC_EXAMPLES_URL}" "${example_out}"
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

  INSTALLED_HBASE_VERSION=`hbase version | perl -ne 'print $1 if /^HBase (.+)$/'`
  if [[ $(echo "${INSTALLED_HBASE_VERSION%.*} < 2.3" | bc -l) == 1 ]]; then
    BIGTABLE_REGISTRY="BigtableAsyncRegistry"
  else
    BIGTABLE_REGISTRY="BigtableConnectionRegistry"
  fi

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
    <value>org.apache.hadoop.hbase.client.${BIGTABLE_REGISTRY}</value>
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

  if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.0" | bc -l) == 1 ]]; then
    configure_bigtable_client_2x || err 'Failed to configure big table 2.x client.'
  else
    configure_bigtable_client_1x || err 'Failed to configure big table 1.x client.'
  fi
}

function install_hbase() {
  case "${DATAPROC_IMAGE_VERSION}" in
    "1.3" | "1.4" | "1.5" )
      if command -v apt-get >/dev/null; then
        # On images prior to Dataproc 2.1, hbase 1.x can be installed from Google's bigtop repositories
        retry_command "apt-get update" || err 'Unable to update packages lists.'
        retry_command "apt-get install -y hbase" || err 'Unable to install HBase.'
      else
        retry_command "yum -y update" || err 'Unable to update packages lists.'
        retry_command "yum -y install hbase" || err 'Unable to install HBase.'
      fi
      ;;

    "2.0" | "2.1" | "2.2" )

      # get hbase tar from official site
      # Variants:
      # * hadoop3-client-bin
      # * hadoop3-bin
      # * client-bin
      # * bin
      echo "preparing to install hbase"
      local HBASE_VERSION="2.3.6"
      local VARIANT="bin"
      local BASENAME="hbase-${HBASE_VERSION}-${VARIANT}.tar.gz"
      echo "hbase dist basename: ${BASENAME}"
      curl -fsSL -o "/tmp/${BASENAME}" --retry-connrefused --retry 3 --retry-max-time 5 \
        "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/${BASENAME}" || err 'Unable to download tar'

      # extract binaries from bundle
      mkdir -p "/tmp/hbase-${HBASE_VERSION}/" "${HBASE_HOME}"
      tar xzf "/tmp/${BASENAME}" --strip-components=1 -C "/tmp/hbase-${HBASE_VERSION}/"
      cp -a "/tmp/hbase-${HBASE_VERSION}/." "${HBASE_HOME}/"

      # install hbase and hbase-config.sh into normal user $PATH
      ln -sf ${HBASE_HOME}/bin/hbase /usr/bin/
      ln -sf ${HBASE_HOME}/bin/hbase-config.sh /usr/bin/
      echo "hbase binary distribution installed"
      ;;
    "*")
      echo "unsupported DATAPROC_IMAGE_VERSION: ${DATAPROC_IMAGE_VERSION}" >&2
      exit 1
      ;;
  esac
}

function main() {
  if [[ ${OS_NAME} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    remove_old_backports
  fi
  install_hbase             || err 'Failed to install HBase.'
  install_bigtable_client   || err 'Unable to install big table client.'
  install_shc               || err 'Failed to install Spark-HBase connector.'
  configure_bigtable_client || err 'Failed to configure big table client.'
}

main
