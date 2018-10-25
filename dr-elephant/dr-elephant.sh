#!/usr/bin/env bash
# Init action for Dr.Elephant
set -x -e

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}
function prepare_env(){
  cat << 'EOF' >> ~/.bashrc
export ACTIVATOR_HOME=/usr/lib/activator-dist-1.3.12/activator-dist-1.3.12
export HADOOP_HOME='/usr/lib/hadoop'
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME='/usr/lib/spark'
export SPARK_CONF_DIR='/usr/lib/spark/conf'
export PATH=$PATH:$HADOOP_HOME:$HADOOP_CONF_DIR:$SPARK_HOME:$SPARK_CONF_DIR:$ACTIVATOR_HOME/bin/
EOF
  readonly enable_cloud_sql_metastore="$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-hive-metastore || echo 'true')"
  cd /root/
  source .bashrc
}

function build(){
  local old_port="val DFS_HTTP_PORT = 50070"
  local new_port="val DFS_HTTP_PORT = $(echo "$(hdfs getconf -confKey dfs.namenode.http-address)" | cut -d ":" -f 2)"

  # Download and install dr elephant and build tools
  git clone https://github.com/linkedin/dr-elephant.git
  wget https://downloads.typesafe.com/typesafe-activator/1.3.12/typesafe-activator-1.3.12.zip
  unzip typesafe-activator-1.3.12.zip -d /usr/lib/activator-dist-1.3.12
  sudo apt-get install apt-transport-https
  echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
  sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
  sudo apt-get update
  yes | sudo apt-get install sbt

  # Fix hardcoded hdfs port problem for 1.3 images
  cd dr-elephant*
  sed -i 's@'"${old_port}"'@'"${new_port}"'@' app/com/linkedin/drelephant/util/SparkUtils.scala

  # Symbolic link to proceed with activator during build
  ln -s /usr/lib/activator-dist-1.3.12/activator-dist-1.3.12/bin/activator activator

  # Install node and required dependencies
  sudo apt-get remove nodejs npm
  curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -
  sudo apt-get install -y nodejs
  npm install -g n
  n stable
  n rm v0.10.29
  npm -v
  npm install -g bower
  npm install -g ember-inflector@^1.9.4
  yes | sudo apt-get install ant
  git clone git://github.com/playframework/play.git
  cd play/framework
  ant
  cd ..; cd ..

  ln -s /usr/bin/nodejs /usr/bin/node || echo "symbolic link already exists"
  cd web; bower --allow-root install; cd ..

  # Disable tests
  sed -i 's|^play_command $OPTS clean test compile dist*|./activator $OPTS clean compile dist|' compile.sh

  # Build dr elephant and move outputs
  bash ./compile.sh compile.conf
  unzip /root/dr-elephant/dist/dr-elephant-2.1.7.zip -d /root/dr-elephant/dist/
}

function configure(){
  local old_java_line='#export JAVA_HOME={JAVA_HOME}'
  local new_java_line='export JAVA_HOME=/usr/bin/java/'
  sed -i 's/^db_password=""/db_password="root-password"/' dist/dr-elephant-2.1.7/app-conf/elephant.conf
  sed -i "s@^$old_java_line@$new_java_line@" /usr/lib/hadoop/etc/hadoop/hadoop-env.sh
  # Setup Fetchers
  cat <<EOF > /root/dr-elephant/dist/dr-elephant-2.1.7/app-conf/FetcherConf.xml
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

<!-- Data fetchers configurations
A Fetcher implements ElephantFetcher interface and help fetch a certain application type data.

Example:
<fetcher>
# Choose the application type that this fetcher is for
<applicationtype>mapreduce</applicationtype>


# Specify the implementation class
<classname>com.linkedin.drelephant.mapreduce.fetchers.MapReduceFetcherHadoop2</classname>
</fetcher>
-->
<fetchers>
<!--
REST based fetcher for Tez jobs which pulls job metrics and data from Timeline Server API
-->

<!--
<fetcher>
<applicationtype>mapreduce</applicationtype>
<classname>com.linkedin.drelephant.mapreduce.fetchers.MapReduceFetcherHadoop2</classname>
<params>
<sampling_enabled>false</sampling_enabled>
</params>
</fetcher>
-->
<!--
This is a replacement for the MapReduceFetcherHadoop2 that attempts to burn
through queues of jobs faster by pulling data directly from HDFS rather than going through
the job history server.

Increasing the param history_log_size_limit_in_mb allows this fetcher to accept larger log
files, but also increase the risk of OutOfMemory error. The default heap size of Dr. Elephant
is 1024MB. To increase this, e.g. to 2048MB, update the below mem conf in app-conf/elephant.conf:
jvm_args="-mem 2048"

To work properly, this fetcher should use the same timezone with the job history server.
If not set, the local timezone will be used.
-->

<fetcher>
<applicationtype>mapreduce</applicationtype>
<classname>com.linkedin.drelephant.mapreduce.fetchers.MapReduceFSFetcherHadoop2</classname>
<params>
<sampling_enabled>false</sampling_enabled>
<history_log_size_limit_in_mb>500</history_log_size_limit_in_mb>
<history_server_time_zone>PST</history_server_time_zone>
</params>
</fetcher>


<!--
FSFetcher for Spark. Loads the eventlog from HDFS and replays to get the metrics and application properties

Param Description:
*event_log_size_limit_in_mb* sets the threshold for the size of the eventlog. Increasing it will necessiate
increase in heap size. default is 100

*event_log_location_uri* can be used to specify the fully qualified uri for the location in hdfs for eventlogs
if this is not specified, the fetcher will try to deduce it from the spark-conf

eg:
<params>
<event_log_size_limit_in_mb>500</event_log_size_limit_in_mb>
<event_log_location_uri>webhdfs://localhost:50070/system/spark-history</event_log_location_uri>
</params>
-->
<fetcher>
<applicationtype>spark</applicationtype>
<classname>com.linkedin.drelephant.spark.fetchers.FSFetcher</classname>
</fetcher>

<!--
This is an experimental fetcher for Spark applications which uses SHS REST API to get application metrics
and WebHDFS to get application properties from eventlogs.

<fetcher>
<applicationtype>spark</applicationtype>
<classname>com.linkedin.drelephant.spark.fetchers.SparkFetcher</classname>
</fetcher>

Param Description (Requires Spark >= 1.5.0):
*use_rest_for_eventlogs* enables the fetcher to get eventlogs via SHS REST API to derive application properties.
*should_process_logs_locally* if use_rest_for_eventlogs is true, then enabling this flag will enable fetcher to just
get eventlogs via SHS REST API and derives application metrics and properties from eventlogs.
Therefore, fetcher does not use other REST calls, which may have significant memory overhead on SHS.

<fetcher>
<applicationtype>spark</applicationtype>
<classname>com.linkedin.drelephant.spark.fetchers.SparkFetcher</classname>
<params>
<use_rest_for_eventlogs>true</use_rest_for_eventlogs>
<should_process_logs_locally>true</should_process_logs_locally>
</params>
</fetcher>
-->
</fetchers>
EOF
  # Enable compress for making metrics accessible by dr elephant
  echo "spark.eventLog.compress = true" >> $SPARK_CONF_DIR/spark-defaults.conf
}


function prepare_mysql(){
  systemctl restart mysql
  mysql -u root -proot-password -e " \
  CREATE DATABASE drelephant;"
}


function run_dr(){
  # Restart History Server
  systemctl restart spark-historyserver
  bash /root/dr-elephant/dist/dr-elephant-2.1.7/bin/start.sh
}

function main(){
  # Install on master node
  [[ "$(/usr/share/google/get_metadata_value attributes/dataproc-role)" == 'Master' ]] || exit 0
  prepare_env || err 'Env configuration failed'
  build || err 'Build step failed'
  configure || err 'Configuration failed'
  prepare_mysql || err 'Could not proceed with mysql'
  run_dr || err 'Cannot launch dr-elephant'
}

main