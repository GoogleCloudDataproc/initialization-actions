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
#
# Initialization action for installing Apache HBase anc configuring it to access
# cloud-bigtable

# Determine the role of this node
set -e
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
PROJECT=$(/usr/share/google/get_metadata_value ../project/project-id)
BIGTABLE_PROJECT=$(/usr/share/google/get_metadata_value attributes/bigtable-project || true)
BIGTABLE_INSTANCE=$(/usr/share/google/get_metadata_value attributes/bigtable-instance || true)
BIGTABLE_PROJECT=${BIGTABLE_PROJECT:-${PROJECT}}
MVN_REPO="https://repo1.maven.org/maven2"

# Keep up to date
BIGTABLE_CLIENT_VERSION='0.9.4'
NETTY_TCNATIVE_VERSION='1.1.33.Fork25'

apt-get update
apt-get install hbase -y
HBASE_VERSION=$(dpkg-query --showformat='${Version}' --show hbase)
HBASE_VERSION=${HBASE_VERSION%.*}

wget -P /usr/lib/hbase/lib \
  "${MVN_REPO}/com/google/cloud/bigtable/bigtable-hbase-shaded/${BIGTABLE_CLIENT_VERSION}/bigtable-hbase-shaded-${BIGTABLE_CLIENT_VERSION}.jar"
wget -P /usr/lib/hbase/lib \
  "${MVN_REPO}/com/google/cloud/bigtable/bigtable-hbase-${HBASE_VERSION}/${BIGTABLE_CLIENT_VERSION}/bigtable-hbase-${HBASE_VERSION}-${BIGTABLE_CLIENT_VERSION}.jar"
wget -P /usr/lib/hbase/lib \
  "${MVN_REPO}/io/netty/netty-tcnative-boringssl-static/${NETTY_TCNATIVE_VERSION}/netty-tcnative-boringssl-static-${NETTY_TCNATIVE_VERSION}-linux-x86_64.jar"

cat << EOF > /etc/hbase/conf/hbase-site.xml
<configuration>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase${HBASE_VERSION/./_}.BigtableConnection</value>
  </property>
  <property>
    <name>google.bigtable.project.id</name>
    <value>${BIGTABLE_PROJECT}</value>
  </property>
  <!--TODO(pmkc): Fix in Bigtable Client-->
  <property>
    <name>google.bigtable.rpc.timeout.ms</name>
    <value>30000</value>
  </property>
$(if [[ -n "${BIGTABLE_INSTANCE}" ]]; then
  cat << EOT
  <property>
    <name>google.bigtable.instance.id</name>
    <value>${BIGTABLE_INSTANCE}</value>
  </property>
EOT
fi)
</configuration>
EOF

# Update Classpaths
cat << 'EOF' >> /etc/hadoop/conf/mapred-env.sh

# HBase Classpath
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/usr/lib/hbase/*"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/usr/lib/hbase/lib/*"
HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:/etc/hbase/conf"
EOF

cat << 'EOF' >> /etc/spark/conf/spark-env.sh

# HBase Classpath
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/hbase/*"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/usr/lib/hbase/lib/*"
SPARK_DIST_CLASSPATH="${SPARK_DIST_CLASSPATH}:/etc/hbase/conf"
EOF
