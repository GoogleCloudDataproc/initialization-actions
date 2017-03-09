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
# Initialization action for installing Apache Oozie on a Google Cloud
# Dataproc cluster. This script will install and configure Oozie to run on the
# master node of a Dataproc cluster. The version of Oozie which is installed
# comes from the BigTop repository. 
#
# You can find more information about Oozie at http://oozie.apache.org/
# For more information in init actions and Google Cloud Dataproc see the Cloud
# Dataproc documentation at https://cloud.google.com/dataproc/init-actions
#
# This script should run in under a few minutes

# Determine the role of this node
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
HOSTNAME=$(hostname)

# Only run on the master node of the cluster
if [[ "${ROLE}" == 'Master' ]]; then
  # Upgrade the repository and install Tomcat
  apt-get update
  apt-get install bigtop-tomcat -y

  # install Oozie from remote repository url
  wget http://dataproc-bigtop-repo.storage.googleapis.com/v1.0-RC0/contrib/o/oozie/oozie_4.2.0-1_all.deb
  wget http://dataproc-bigtop-repo.storage.googleapis.com/v1.0-RC0/contrib/o/oozie/oozie-client_4.2.0-1_all.deb

  # install with force
  dpkg --force-all -i oozie-client_4.2.0-1_all.deb
  dpkg --force-all -i oozie_4.2.0-1_all.deb

  # The ext library is needed to enable the Oozie web console
  wget http://archive.cloudera.com/gplextras/misc/ext-2.2.zip
  unzip ext-2.2.zip
  cp -ax ext-2.2 /usr/lib/oozie/libext/

  # Create the Oozie database
  sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run

  # Hadoop must allow impersonation for Oozie to work properly
  cat > core-site-patch.xml <<'EOF'
  <property>
    <name>hadoop.proxyuser.oozie.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.oozie.groups</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.${user.name}.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.${user.name}.groups</name>
    <value>*</value>
  </property>
EOF
  sed -i '/<\/configuration>/e cat core-site-patch.xml' \
      /etc/hadoop/conf/core-site.xml

  # Clean up temporary files
  rm -rf ext-2.2 ext-2.2.zip core-site-patch.xml

  # Required for Oozie sharedlib setup
  wget http://central.maven.org/maven2/commons-io/commons-io/2.3/commons-io-2.3.jar
  wget http://central.maven.org/maven2/org/apache/htrace/htrace-core4/4.2.0-incubating/htrace-core4-4.2.0-incubating.jar
  wget http://central.maven.org/maven2/jline/jline/2.12/jline-2.12.jar
  wget http://central.maven.org/maven2/org/apache/hive/hive-common/1.2.1/hive-common-1.2.1.jar

  # Remove HTrace 3.1.0 and Hadoop Auth 2.7.1
  rm /usr/lib/oozie/lib/htrace-core-3.1.0-incubating.jar
  rm /usr/lib/oozie/lib/hadoop-auth-2.7.1.jar

  # Copy Commons-IO and new HTrace
  mv commons-io-2.3.jar /usr/lib/oozie/lib/
  mv htrace-core4-4.2.0-incubating.jar /usr/lib/oozie/lib/

  # Delete the invalid Hadoop HDFS library
  rm /usr/lib/oozie/lib/hadoop-hdfs.jar
  ln -s /usr/lib/hadoop/client/hadoop-hdfs-client-2.8.0-SNAPSHOT.jar /usr/lib/oozie/lib/hadoop-hdfs-client.jar

  # HDFS and MapReduce must be cycled; restart to clean things up
  service hadoop-hdfs-namenode restart
  service hadoop-hdfs-secondarynamenode restart
  service hadoop-mapreduce-historyserver restart
  service hadoop-yarn-resourcemanager restart
  service hive-server2 restart
  service oozie start

  # Create sharedlib
  oozie-setup sharelib create -fs hdfs://${HOSTNAME}

  # Update available sharedlib
  oozie admin -sharelibupdate -oozie http://${HOSTNAME}:11000/oozie

  # Figure out sharelib directory on HDFS
  SHARELIB_DIR=$(hdfs dfs -ls hdfs:///user/oozie/share/lib/|tail -n 1|awk '{print $8}')

  # Remove the old jline dependency
  hdfs dfs -rm ${SHARELIB_DIR}/hive2/jline-0.9.94.jar

  # Download and install missing dependencies
  hdfs dfs -put jline-2.12.jar ${SHARELIB_DIR}/hive2/
  hdfs dfs -put hive-common-1.2.1.jar ${SHARELIB_DIR}/hive2/

  # Update available sharedlib one more time
  oozie admin -sharelibupdate -oozie http://${HOSTNAME}:11000/oozie
fi
