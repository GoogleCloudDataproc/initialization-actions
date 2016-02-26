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

## TODO: complete doc

set -x -e

hadoop_conf_dir="/etc/hadoop/conf"


# Determine the role of this node
ROLE=$(/usr/share/google/get_metadata_value attributes/role)

# Only run on the master node of the cluster
if [[ "${ROLE}" == 'Master' ]]; then
  apt-get update
  apt-get install hue

 
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
      ${hadoop_conf_dir}/hdfs-site.xml


# Clean up temporary fles
rm -rf hdfs-site-patch.xml

# Restart HDFS
./usr/lib/hadoop/libexec/init-hdfs.sh

# Restart Hue
service hue stop
service hue start


set +x +e
