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

set -x -e

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

function main() {
# Determine the role of this node
local role=$(/usr/share/google/get_metadata_value attributes/dataproc-role)

# Only run on the master node of the cluster
if [[ "${role}" != 'Master' ]]; then
  exit 0
fi

# Upgrade the repository and install Oozie
update_apt_get
apt-get install oozie oozie-client -y || err 'Unable to install oozie-client'

# The ext library is needed to enable the Oozie web console
wget http://archive.cloudera.com/gplextras/misc/ext-2.2.zip || err 'Unable to download ext-2.2.zip'
unzip ext-2.2.zip
ln -s ext-2.2 /var/lib/oozie/ext-2.2/ext-2.2

# Create the Oozie database
sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run

# Hadoop must allow impersonation for Oozie to work properly
bdconfig set_property \
    --configuration_file 'core-site-patch.xml' \
    --name 'hadoop.proxyuser.oozie.hosts' --value '*' \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.oozie.hosts'
bdconfig set_property \
    --configuration_file 'core-site-patch.xml' \
    --name 'hadoop.proxyuser.oozie.groups' --value '*' \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.oozie.groups'
bdconfig set_property \
    --configuration_file 'core-site-patch.xml' \
    --name 'hadoop.proxyuser.${user.name}.hosts' --value '*' \
    --clobber \
    || err 'Unable to set hadoop.proxyuser.${user.name}.hosts'
bdconfig set_property \
    --configuration_file 'core-site-patch.xml' \
    --name 'hadoop.proxyuser.${user.name}.groups' --value '*' \
    --clobber \
    || err 'hadoop.proxyuser.${user.name}.groups'
sed -i '/<\/configuration>/e cat core-site-patch.xml' \
    /etc/hadoop/conf/core-site.xml

# Install share lib
tar -xvzf /usr/lib/oozie/oozie-sharelib.tar.gz
ln -s share /usr/lib/oozie/share
hadoop fs -mkdir -p /user/oozie/
hadoop fs -put share/ /user/oozie/

# Clean up temporary fles
rm -rf ext-2.2 ext-2.2.zip core-site-patch.xml share oozie-sharelib.tar.gz

# HDFS and YARN must be cycled; restart to clean things up
systemctl restart hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode \
  hadoop-yarn-resourcemanager oozie \
  || err 'HDFS and YARN restart actions failed'

}

main
