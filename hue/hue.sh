#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -x -e

# Determine the role of this node
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)


# Only run on the master node of the cluster
if [[ "${ROLE}" == 'Master' ]]; then
  apt-get update
  apt-get install hue -y

  cat > core-site-patch.xml <<EOF
  <property> 
    <name>hadoop.proxyuser.hue.hosts</name> 
    <value>*</value> 
  </property> 
  <property> 
    <name>hadoop.proxyuser.hue.groups</name> 
    <value>*</value> 
  </property> 
EOF
  sed -i '/<\/configuration>/e cat core-site-patch.xml' \
     /etc/hadoop/conf/core-site.xml
  
 
  cat > hdfs-site-patch.xml <<EOF
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
  
  <property>
    <name>hadoop.proxyuser.hue.hosts</name>
    <value>*</value>
  </property>
  
  <property>
    <name>hadoop.proxyuser.hue.groups</name>
    <value>*</value>
  </property>
EOF

  sed -i '/<\/configuration>/e cat hdfs-site-patch.xml' \
       /etc/hadoop/conf/hdfs-site.xml
       
  cat >> /etc/hue/conf/hue.ini <<EOF
    # Defaults to $HADOOP_MR1_HOME or /usr/lib/hadoop-0.20-mapreduce
    hadoop_mapred_home=/usr/lib/hadoop-mapreduce 
EOF
       
  # Replace localhost with hostname.
  sed -i "s/#*\([^#]*=.*\)localhost/\1$(hostname --fqdn)/" /etc/hue/conf/hue.ini

  # Clean up temporary fles
  rm -rf hdfs-site-patch.xml core-site-patch.xml hue-patch.ini

  # Restart HDFS
  /usr/lib/hadoop/libexec/init-hdfs.sh

  # Restart Hue
  service hue restart
fi

set +x +e
