# Copyright 2014 Google Inc. All Rights Reserved.
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


# ambari_manual_env.sh
#
# Extension installing Apache Ambari on the cluster allowing the user to
# manually log in and provision and configure the clusters software.
# This installs but does not configure the GCS connector.

########################################################################
## There should be nothing to edit here, use ambari.conf              ##
########################################################################


# Import hadoop2_env.sh just for the GCS_CONNECTOR_JAR.
import_env hadoop2_env.sh

# Default to 4 workers plus master for good spreading of master daemons.
NUM_WORKERS=4
# Use CentOS instead of Debian.
GCE_IMAGE='centos-6'

# Create attached storage
USE_ATTACHED_PDS=true
WORKER_ATTACHED_PDS_SIZE_GB=1500
MASTER_ATTACHED_PD_SIZE_GB=1500

# Install the full Java JDK. Most services need it
INSTALL_JDK_DEVEL=true
JAVA_HOME=/etc/alternatives/java_sdk

## import configuration overrides
import_env platforms/hdp/ambari.conf

## Version of Ambari and location of YUM package repository
AMBARI_VERSION="${AMBARI_VERSION:-1.7.0}"
AMBARI_REPO=${AMBARI_REPO:-http://public-repo-1.hortonworks.com/ambari/centos6/1.x/updates/${AMBARI_VERSION}/ambari.repo}

## If 'true', URLs for web interfaces, such as the jobtracker will below
## linked from Ambari with the public IP.
## Default is false. You will need to SSH to reach the host in this case.
AMBARI_PUBLIC=${AMBARI_PUBLIC:-false}
normalize_boolean 'AMBARI_PUBLIC'

# Since we'll be using HDFS as the default_fs, set some reasonably beefy
# disks.

readonly DEFAULT_FS='hdfs'
GCS_CACHE_CLEANER_USER='hdfs'
GCS_CACHE_CLEANER_LOG_DIRECTORY="/var/log/hadoop/${GCS_CACHE_CLEANER_USER}"
GCS_CACHE_CLEANER_LOGGER='INFO,RFA'
HADOOP_CONF_DIR="/etc/hadoop/conf"
HADOOP_INSTALL_DIR="/usr/local/lib/hadoop"

# For interacting with Ambari Server API
AMBARI_API="http://localhost:8080/api/v1"
AMBARI_CURL='curl -su admin:admin -H X-Requested-By:ambari'
MASTER_UI_PORTS=('8080')

import_env platforms/hdp/ambari_functions.sh

UPLOAD_FILES=(
  'hadoop2_env.sh'
  'libexec/hadoop_helpers.sh'
  'platforms/hdp/ambari_functions.sh'
  'platforms/hdp/ambari.conf'
  'platforms/hdp/resources/public-hostname-gcloud.sh'
  'platforms/hdp/resources/thp-disable.sh'
)

COMMAND_GROUPS+=(
  "ambari-setup:
     libexec/mount_disks.sh
     libexec/install_java.sh
     libexec/setup_hadoop_user.sh
     platforms/hdp/install_ambari.sh
  "
)

COMMAND_STEPS=(
  'ambari-setup,ambari-setup'
  'deploy-master-nfs-setup,*'
  'deploy-client-nfs-setup,deploy-client-nfs-setup'
)
