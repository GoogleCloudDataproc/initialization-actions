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

# Places files into expected files; generates a project_properties.sh file
# which other scripts are designed to use.

set -o nounset
set -o errexit

mkdir -p ${MASTER_PACKAGE_DIR}/conf/hive
mv hive-site.xml ${MASTER_PACKAGE_DIR}/conf/hive/

# Dynamically generated a project_properties.sh file which only contains the
# environment variables which must be derived from existing hadoop deployment
# variables.
cat << EOF >> project_properties.sh
SUPPORTED_HDPTOOLS='hive pig'
ZONE=${GCE_ZONE}
MASTER=${MASTER_HOSTNAME}
HADOOP_HOME=${HADOOP_INSTALL_DIR}
EOF

# Explicitly set a schemeless working directory, otherwise as of Pig 0.12.0
# PigInputFormat fails to use input paths which are not from the "default"
# FileSystem. No need to clobber existing working-directory settings.
bdconfig merge_configurations \
    --configuration_file ${HADOOP_CONF_DIR}/mapred-site.xml \
    --source_configuration_file pig-mapred-template.xml \
    --resolve_environment_variables \
    --create_if_absent \
    --noclobber
