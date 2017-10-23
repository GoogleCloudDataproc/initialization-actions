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
if [[ "${ROLE}" != 'Master' ]]; then
  exit 0
fi

# Install hue
apt-get update
apt-get install -t jessie-backports hue -y

# Stop hue
systemctl stop hue

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

# If oozie installed, add hue as proxy user for oozie
if [[ -f /etc/oozie/conf/oozie-site.xml ]];
then
  cat > oozie-site-patch.xml <<EOF
<property>
  <name>oozie.service.ProxyUserService.proxyuser.hue.hosts</name>
  <value>*</value>
</property>

<property>
  <name>oozie.service.ProxyUserService.proxyuser.hue.groups</name>
  <value>*</value>
</property>
EOF

  sed -i '/<\/configuration>/e cat oozie-site-patch.xml' \
      /etc/oozie/conf/oozie-site.xml

  rm oozie-site-patch.xml
  systemctl restart oozie
fi

# Fix webhdfs_url
OLD_HDFS_URL='## webhdfs_url\=http:\/\/localhost:50070'
NEW_HDFS_URL='webhdfs_url\=http:\/\/'"$(hdfs getconf -confKey  dfs.namenode.http-address)"
sed -i 's/'"$OLD_HDFS_URL"'/'"$NEW_HDFS_URL"'/' /etc/hue/conf/hue.ini

# Uncomment every line containing localhost, replacing localhost with the FQDN
sed -i "s/#*\([^#]*=.*\)localhost/\1$(hostname --fqdn)/" /etc/hue/conf/hue.ini

# Comment out any duplicate resourcemanager_api_url fields after the first one
sed -i '0,/resourcemanager_api_url/! s/resourcemanager_api_url/## resourcemanager_api_url/' \
    /etc/hue/conf/hue.ini

# Clean up temporary fles
rm -rf hdfs-site-patch.xml core-site-patch.xml hue-patch.ini

# Make hive warehouse directory
hdfs dfs -mkdir /user/hive/warehouse

# Configure Desktop Database to use mysql
OLD_SETTINGS='## engine=sqlite3(\s+)## host=(\s+)## port=(\s+)## user=(\s+)## password='
NEW_SETTINGS='engine=mysql$1host=127.0.0.1$2port=3306$3user=hue$4password=hue-password'
perl -i -0777 -pe 's/'"$OLD_SETTINGS"'/'"$NEW_SETTINGS"'/' /etc/hue/conf/hue.ini

# Comment out sqlite3 configuration
sed -i 's/engine=sqlite3/## engine=sqlite3/' /etc/hue/conf/hue.ini

# Set database name to hue
sed -i 's/name=\/var\/lib\/hue\/desktop.db/name=hue/' /etc/hue/conf/hue.ini

# Export passwords
export MYSQL_PASSWORD='root-password'
export HUE_PASSWORD='hue-password'

# Create database, give hue user permissions
mysql -u root -proot-password -e " \
  CREATE DATABASE hue; \
  CREATE USER 'hue'@'localhost' IDENTIFIED BY '$HUE_PASSWORD'; \
  GRANT ALL PRIVILEGES ON hue.* TO 'hue'@'localhost';"

# Restart mysql
systemctl restart mysql

# Hue creates all needed tables
/usr/lib/hue/build/env/bin/hue syncdb --noinput
/usr/lib/hue/build/env/bin/hue migrate

# Restart servers
systemctl restart hadoop-hdfs-namenode hadoop-yarn-resourcemanager hue mysql
