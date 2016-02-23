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

# Only run on the master node of the cluster
if [[ "${ROLE}" == 'Master' ]]; then
  # Upgrade the repository and install Oozie
  apt-get update
  apt-get install oozie oozie-client -y
  
  # The ext library is needed to enable the Oozie web console
  wget http://archive.cloudera.com/gplextras/misc/ext-2.2.zip
  unzip ext-2.2.zip
  cp -ax ext-2.2 /usr/lib/oozie/libext/
  
  # Create the Oozie database
  sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run
  
  # Hadoop must allow impersonation for Oozie to work properly
  cat > core-site-patch.xml <<EOF
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
  
  # Clean up temporary fles
  rm -rf ext-2.2 ext-2.2.zip core-site-patch.xml
  
  # HDFS and MapReduce must be cycled; restart to clean things up
  reboot
fi