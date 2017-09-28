#!/bin/bash
#  Copyright 2015 Google, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
set -x -e

#  Define important variables
tez_version="0.7.0"
protobuf_version="2.5.0"
tez_hdfs_path="/apps/tez"
tez_jars="/usr/lib/tez"
tez_conf_dir="/etc/tez/conf"
hadoop_conf_dir="/etc/hadoop/conf"
hive_conf_dir="/etc/hive/conf"
role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

# Let spark continue using the existing hive configuration, as it will
# not want to use hive on tez.
cp /etc/hive/conf/* /etc/spark/conf/
# Remove lines containing /etc/hive/conf from spark-env.sh
sudo sed -i '\#CLASSPATH=.*/etc/hive/conf#d' /etc/spark/conf/spark-env.sh

if [[ "${role}" == 'Master' ]]; then
  # Install Tez and YARN Application Timeline Server.
  # Install node/npm to run HTTP server for Tez UI.
  apt-get install tez hadoop-yarn-timelineserver nodejs-legacy npm -y

  # Stage Tez
  hadoop fs -mkdir -p ${tez_hdfs_path}
  hadoop fs -copyFromLocal ${tez_jars}/* ${tez_hdfs_path}/

  # Update the hadoop-env.sh
  {
    echo "export TEZ_CONF_DIR=/etc/tez/"
    echo "export tez_jars=${tez_jars}"
    echo "HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:${tez_conf_dir}:${tez_jars}/*:${tez_jars}/lib/*"
  } >> /etc/hadoop/conf/hadoop-env.sh

  # Configure YARN to enable the Application Timeline Server,
  # and configure the Tez UI to push logs to the ATS.
  # See https://tez.apache.org/tez-ui.html for more information.
  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.http.filter.initializers' --value 'org.apache.hadoop.security.HttpCrossOriginFilterInitializer' \
    --clobber
  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/yarn-site.xml" \
    --name 'yarn.resourcemanager.webapp.cross-origin.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/yarn-site.xml" \
    --name 'yarn.timeline-service.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/yarn-site.xml" \
    --name 'yarn.timeline-service.hostname' --value "$HOSTNAME" \
    --clobber
  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/yarn-site.xml" \
    --name 'yarn.timeline-service.http-cross-origin.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/yarn-site.xml" \
    --name 'yarn.resourcemanager.system-metrics-publisher.enabled' --value 'true' \
    --clobber
  bdconfig set_property \
    --configuration_file "${tez_conf_dir}/tez-site.xml" \
    --name 'tez.history.logging.service.class' --value 'org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService' \
    --clobber
  bdconfig set_property \
    --configuration_file "${tez_conf_dir}/tez-site.xml" \
    --name 'tez.tez-ui.history-url.base' --value "http://$HOSTNAME:9999/tez-ui/" \
    --clobber

  # Update hive to use tez as execution engine
  bdconfig set_property \
    --configuration_file "${hive_conf_dir}/hive-site.xml" \
    --name 'hive.execution.engine' --value 'tez' \
    --clobber

  # Set up service for Tez UI on port 9999 at the path /tez-ui
  npm install -g http-server forever forever-service
  unzip /usr/lib/tez/tez-ui-*.war -d /usr/lib/tez/tez-ui
  forever-service install tez-ui --script /usr/local/bin/http-server -o "/usr/lib/tez/ -p 9999"

  # Restart daemons

  systemctl daemon-reload

  # Start tez ui
  systemctl enable tez-ui
  systemctl restart tez-ui
  systemctl status tez-ui  # Ensure it started successfully

  # Restart resource manager
  systemctl restart hadoop-yarn-resourcemanager
  systemctl status hadoop-yarn-resourcemanager  # Ensure it started successfully

  # Enable timeline server
  systemctl enable hadoop-yarn-timelineserver
  systemctl restart hadoop-yarn-timelineserver
  systemctl status hadoop-yarn-timelineserver  # Ensure it started successfully

  # Restart hive server2
  systemctl restart hive-server2
  systemctl status hive-server2  # Ensure it started successfully
fi

set +x +e
