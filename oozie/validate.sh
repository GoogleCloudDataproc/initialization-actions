#!/bin/bash
echo "starting validation script"
namenode=$(bdconfig get_property_value --configuration_file /etc/hadoop/conf/core-site.xml --name fs.default.name 2>/dev/null)
hostname="$(hostname)"
sudo -u hdfs hadoop dfsadmin -safemode leave &> /dev/null
cp /usr/share/doc/oozie/oozie-examples.tar.gz ~
tar -zxvf oozie-examples.tar.gz
cat << EOF > /home/$(whoami)/examples/apps/map-reduce/job.properties
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
hadoop f
nameNode=${namenode}:8020
jobTracker=${hostname}:8032
queueName=default
examplesRoot=examples

oozie.wf.application.path=${namenode}/user/$(whoami)/examples/apps/map-reduce/workflow.xml
outputDir=map-reduce
oozie.use.system.libpath=true
EOF

hdfs dfs -mkdir -p /user/$(whoami)/
hadoop fs -put ~/examples/ /user/$(whoami)/

echo "---------------------------------"
echo "Starting validation on ${hostname}"
oozie job -oozie http://localhost:11000/oozie -config /home/$(whoami)/examples/apps/map-reduce/job.properties -run
oozie jobs -oozie http://localhost:11000/oozie -localtime -len 2 | grep "No Jobs match your criteria!"

if [[ $? == 1 ]]; then
  exit 0
else
  exit 1
fi
echo "---------------------------------"
