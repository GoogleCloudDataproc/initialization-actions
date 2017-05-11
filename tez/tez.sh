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
hive_conf_dir="/etc/hive/conf"
role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

if [[ "${role}" == 'Master' ]]; then
  #  Install Tez
  apt-get install tez -y
  
  #  Stage Tez
  hadoop fs -mkdir -p ${tez_hdfs_path}
  hadoop fs -copyFromLocal ${tez_jars}/* ${tez_hdfs_path}/

  #  Update the hadoop-env.sh
  {
    echo "export TEZ_CONF_DIR=/etc/tez/"
    echo "export tez_jars=${tez_jars}"
    echo "HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:${tez_conf_dir}:${tez_jars}/*:${tez_jars}/lib/*"
  } >> /etc/hadoop/conf/hadoop-env.sh
fi

# Update hive to use tez as execution engine
bdconfig set_property \
  --configuration_file "${hive_conf_dir}/hive-site.xml" \
  --name 'hive.execution.engine' --value 'tez' \
  --clobber
systemctl restart hive-server2

set +x +e
