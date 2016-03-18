#!/bin/bash
# Copyright 2015 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.set -x -e

# This init script installs Apache Zeppelin on the master node of a Cloud
# Dataproc cluster. Zeppelin is also configured based on the size of your
# cluster and the versions of Spark/Hadoop which are installed.
set -x -e

# Get executor memory value
EXECUTOR_MEMORY="$(grep spark.executor.memory /etc/spark/conf/spark-defaults.conf | awk '{print $2}')"

# Set these Spark and Hadoop versions based on your Dataproc version
SPARK_VERSION="1.6.0"
HADOOP_VERSION="2.7.2"

# Only run on the master node
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
if [[ "${ROLE}" == 'Master' ]]; then
	
  # Install Maven 3
  mkdir -p /tmp/maven
  cd /tmp/maven
  wget "ftp://mirror.reverse.net/pub/apache/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz" -P /tmp/
  tar -xf /tmp/apache-maven-3.3.3-bin.tar.gz -C /usr/lib/
  ln -s /usr/lib/apache-maven-3.3.3 /usr/lib/apache-maven
  ln -s /usr/lib/apache-maven/bin/mvn /usr/bin/mvn

  # Install dependencies
  apt-get install -y git vim emacs nodejs npm
  ln -s /usr/bin/nodejs /usr/bin/node
  npm update -g npm
  npm install -g grunt-cli
  npm install -g grunt
  npm install -g bower

  # Install zeppelin
  cd /usr/lib/
  git clone https://github.com/apache/incubator-zeppelin.git
  cd incubator-zeppelin
  # Even with Hadoop 2.7, -Phadoop-2.6 should be used.
  mvn clean install -DskipTests "-Dspark.version=$SPARK_VERSION" "-Dhadoop.version=$HADOOP_VERSION" -Pyarn -Phadoop-2.6 -Pspark-1.6 -Ppyspark
  mkdir -p logs run conf
  cat > conf/zeppelin-env.sh <<EOF
#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export MASTER="yarn-client" # Spark master url. eg. spark://master_addr:7077. Leave empty if you want to use local mode.
export ZEPPELIN_JAVA_OPTS="-Dhdp.version=2.7.2" # Additional jvm options. for example, export ZEPPELIN_JAVA_OPTS="-Dspark.executor.memory=8g -Dspark.cores.max=16"
export ZEPPELIN_MEM=" "  # Zeppelin jvm mem options Default -Xmx1024m -XX:MaxPermSize=512m


#### Spark interpreter configuration ####

## Use provided spark installation ##
## defining SPARK_HOME makes Zeppelin run spark interpreter process using spark-submit
##

export SPARK_HOME="/usr/lib/spark" # (required) When it is defined, load it instead of Zeppelin embedded Spark libraries
export SPARK_SUBMIT_OPTIONS="--executor-memory $EXECUTOR_MEMORY"                   # (optional) extra options to pass to spark submit. eg) "--driver-memory 512M --executor-memory 1G".

## Use embedded spark binaries ##
## without SPARK_HOME defined, Zeppelin still able to run spark interpreter process using embedded spark binaries.
## however, it is not encouraged when you can define SPARK_HOME
##

export HADOOP_CONF_DIR="/etc/hadoop/conf" # yarn-site.xml is located in configuration directory in HADOOP_CONF_DIR.
# Pyspark (supported with Spark 1.2.1 and above)
# To configure pyspark, you need to set spark distribution's path to 'spark.home' property in Interpreter setting screen in Zeppelin GUI

export PYSPARK_PYTHON="/usr/bin/python" # path to the python command. must be the same path on the driver(Zeppelin) and all workers.
export PYTHONPATH="/usr/bin/python"

## Spark interpreter options ##
##
# export ZEPPELIN_SPARK_USEHIVECONTEXT  # Use HiveContext instead of SQLContext if set true. true by default.
# export ZEPPELIN_SPARK_CONCURRENTSQL   # Execute multiple SQL concurrently if set true. false by default.
# export ZEPPELIN_SPARK_MAXRESULT       # Max number of SparkSQL result to display. 1000 by default.

EOF

  cp /etc/hive/conf/hive-site.xml conf/
  chmod -R a+w conf logs run

  # Let Zeppelin create the conf/interpreter.json file
  /usr/lib/incubator-zeppelin/bin/zeppelin-daemon.sh start
  sleep 20s
  /usr/lib/incubator-zeppelin/bin/zeppelin-daemon.sh stop

  # Force the spark.executor.memory to be inherited from the environment
  sed -i 's/"spark.executor.memory": "512m",/"spark.executor.memory": "",/' /usr/lib/incubator-zeppelin/conf/interpreter.json
  /usr/lib/incubator-zeppelin/bin/zeppelin-daemon.sh start
fi

set +x +e
