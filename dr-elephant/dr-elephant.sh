#!/usr/bin/env bash

# Init action for Dr.Elephant

set -euxo pipefail

readonly TYPESAFE_ACTIVATOR_URL=https://downloads.typesafe.com/typesafe-activator/1.3.12/typesafe-activator-1.3.12.zip
readonly DR_ELEPHANT_REVISION=bdf9adeea91264aefabebd392d63602a130a3f05

MASTER_HOSTNAME=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_HOSTNAME

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function build() {
  # Download and install Typesafe Activator
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    ${TYPESAFE_ACTIVATOR_URL} -O /tmp/typesafe-activator.zip
  unzip -q /tmp/typesafe-activator.zip -d /tmp/
  mv /tmp/activator-dist-* /tmp/typesafe-activator
  export PATH=${PATH}:/tmp/typesafe-activator/bin/

  # Download and install Dr. Elephant
  git clone https://github.com/linkedin/dr-elephant.git /tmp/dr-elephant
  pushd /tmp/dr-elephant
  git reset --hard ${DR_ELEPHANT_REVISION}

  # Install dependencies for new Dr. Elephant UI
  curl -sL https://deb.nodesource.com/setup_8.x | bash -
  apt-get install -y nodejs
  npm install -g bower
  pushd web
  bower --allow-root install
  popd

  # Fix hardcoded HDFS port problem for 1.3 images
  local dfs_port
  dfs_port=$(hdfs getconf -confKey dfs.namenode.http-address | cut -d ":" -f 2)
  sed -i "s/val DFS_HTTP_PORT = [0-9]\+/val DFS_HTTP_PORT = ${dfs_port}/" \
    app/com/linkedin/drelephant/util/SparkUtils.scala

  # Disable tests
  sed -i 's/ $OPTS clean compile test $extra_commands/ $OPTS clean compile $extra_commands/' compile.sh

  # Set Hadoop and Spark versions
  # TODO: fix build with overriden Hadoop and Spark versions
  #  local hadoop_version
  #  hadoop_version=$(hadoop version 2>&1 | sed -n 's/.*Hadoop[[:blank:]]\+\([0-9]\+\.[0-9]\.[0-9]\+\+\).*/\1/p' | head -n1)
  #  local spark_version
  #  spark_version=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\.[0-9]\+\+\).*/\1/p' | head -n1)
  #  sed -i "s/hadoop_version=[0-9.]\+/hadoop_version=${hadoop_version}/" compile.conf
  #  sed -i "s/spark_version=[0-9.]\+/spark_version=${spark_version}/" compile.conf

  # Build Dr. Elephant and move outputs
  bash compile.sh compile.conf
  unzip -q dist/dr-elephant-*.zip -d dist-unpacked/
  mv dist-unpacked/dr-elephant-* /opt/dr-elephant

  popd
}

function configure() {
  sed -i 's/^db_password=""/db_password="root-password"/' /opt/dr-elephant/app-conf/elephant.conf

  # Setup Fetchers
  cat <<EOF >/opt/dr-elephant/app-conf/FetcherConf.xml
<?xml version="1.0" encoding="UTF-8"?>

<!--
Copyright 2016 LinkedIn Corp.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

<fetchers>
  <!--
  <fetcher>
    <applicationtype>tez</applicationtype>
    <classname>com.linkedin.drelephant.tez.fetchers.TezFetcher</classname>
  </fetcher>
  -->
  <fetcher>
    <applicationtype>mapreduce</applicationtype>
    <classname>com.linkedin.drelephant.mapreduce.fetchers.MapReduceFetcherHadoop2</classname>
    <params>
      <sampling_enabled>false</sampling_enabled>
    </params>
  </fetcher>
  <fetcher>
    <applicationtype>mapreduce</applicationtype>
    <classname>com.linkedin.drelephant.mapreduce.fetchers.MapReduceFSFetcherHadoop2</classname>
    <params>
      <sampling_enabled>false</sampling_enabled>
      <history_log_size_limit_in_mb>500</history_log_size_limit_in_mb>
      <history_server_time_zone>UTC</history_server_time_zone>
    </params>
  </fetcher>
  <fetcher>
    <applicationtype>spark</applicationtype>
    <classname>com.linkedin.drelephant.spark.fetchers.FSFetcher</classname>
  </fetcher>
  <fetcher>
    <applicationtype>spark</applicationtype>
    <classname>com.linkedin.drelephant.spark.fetchers.SparkFetcher</classname>
    <params>
      <use_rest_for_eventlogs>true</use_rest_for_eventlogs>
      <should_process_logs_locally>true</should_process_logs_locally>
    </params>
  </fetcher>
  <!--
  <fetcher>
    <applicationtype>tony</applicationtype>
    <classname>com.linkedin.drelephant.tony.fetchers.TonyFetcher</classname>
  </fetcher>
  -->
</fetchers>
EOF

  bdconfig set_property \
    --configuration_file "/opt/dr-elephant/app-conf/GeneralConf.xml" \
    --name 'drelephant.analysis.backfill.enabled' --value 'true' \
    --clobber

  # Enable compression to make metrics accessible by Dr. Elephant
  echo "spark.eventLog.compress = true" >>"/usr/lib/spark/conf/spark-defaults.conf"
}

function prepare_mysql() {
  systemctl restart mysql
  mysql -u root -proot-password -e "CREATE DATABASE drelephant;"
}

function run_dr() {
  # Restart History Server
  systemctl restart spark-history-server
  bash /opt/dr-elephant/bin/start.sh
}

# Install on master node
if [[ "${HOSTNAME}" == "${MASTER_HOSTNAME}" ]]; then
  build || err 'Build step failed'
  configure || err 'Configuration failed'
  prepare_mysql || err 'Could not proceed with mysql'
  run_dr || err 'Cannot launch dr-elephant'
fi
