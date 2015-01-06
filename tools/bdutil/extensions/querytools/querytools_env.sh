# Copyright 2013 Google Inc. All Rights Reserved.
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

# This file contains environment-variable overrides to be used in conjunction
# with bdutil_env.sh in order to deploy a Hadoop cluster with Pig and Hive
# installed, using the Cloud Solutions sampleapp.
# Usage: ./bdutil deploy extensions/querytools/querytools_env.sh

# Set the default filesystem to be 'hdfs' since Pig and Hive will tend to rely
# on multi-stage pipelines more heavily then plain Hadoop MapReduce, and thus
# be vulnerable to eventual list consistency. Okay to read initially from GCS
# using explicit gs:// URIs and likewise to write the final output to GCS,
# letting any intermediate cross-stage items get stored in HDFS temporarily.
DEFAULT_FS='hdfs'

# URIs of tarballs to install.
PIG_TARBALL_URI='gs://querytools-dist/pig-0.12.0.tar.gz'
HIVE_TARBALL_URI='gs://querytools-dist/hive-0.12.0-bin.tar.gz'

# Constants normally in project_properties.sh from the sampleapp, but which we
# can propagate out here as shared environment variables instead.
HADOOP_MAJOR_VERSION='1'
HADOOP_USER='hadoop'
HADOOP_GROUP='hadoop'
HDP_USER='hadoop'
HDP_USER_HOME='/home/hadoop'
MASTER_INSTALL_DIR='/home/hadoop'
PACKAGES_DIR='packages'
SCRIPTS_DIR='scripts'
MASTER_PACKAGE_DIR='/tmp/hdp_tools'
HDFS_TMP_DIR='/tmp'
HADOOP_TMP_DIR='/hadoop/tmp'

# File dependencies to be used by the scripts.
UPLOAD_FILES+=(
  'extensions/querytools/pig-mapred-template.xml'
  'sampleapps/querytools/conf/hive/hive-site.xml'
  'sampleapps/querytools/scripts/common_utils.sh'
  'sampleapps/querytools/scripts/package_utils.sh'
)
COMMAND_GROUPS+=(
  "install_querytools:
     extensions/querytools/prepare_files.sh
     sampleapps/querytools/scripts/setup-packages__at__master.sh
     sampleapps/querytools/scripts/setup-hdfs-for-hdtools__at__master.sh
     extensions/querytools/setup_profiles.sh
  "
)

# Querytools installation only needs to run on master.
COMMAND_STEPS+=(
  'install_querytools,*'
)
