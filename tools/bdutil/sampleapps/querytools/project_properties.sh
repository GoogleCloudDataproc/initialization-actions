#!/bin/bash
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Begin: edit these values to set up your cluster
# GCS bucket for packages
readonly GCS_PACKAGE_BUCKET={{{{ bucket_name }}}}
# Zone of the Hadoop master instance
readonly ZONE={{{{ zone_id }}}}
# Hadoop master instance name
readonly MASTER={{{{ master_hostname }}}}

# Subdirectory in cloud storage where packages are pushed at initial setup
readonly GCS_PACKAGE_DIR=hdp_tools

# Full GCS URIs of the Pig and Hive tarballs, if packages-to-gcs__at__host.sh
# is used; alternatively, these can be set to other pre-existing GCS paths
readonly SUPPORTED_HDPTOOLS="hive pig"
readonly TARBALL_BASE="gs://$GCS_PACKAGE_BUCKET/$GCS_PACKAGE_DIR/packages"
readonly HIVE_TARBALL_URI="$TARBALL_BASE/hive/hive-*.tar.gz"
readonly PIG_TARBALL_URI="$TARBALL_BASE/pig/pig-*.tar.gz"

# Directory on master where hadoop is installed
readonly HADOOP_HOME=/home/hadoop/hadoop

# Set to the major version of hadoop ("1" or "2")
readonly HADOOP_MAJOR_VERSION="1"

# Hadoop username and group on Compute Engine Cluster
readonly HADOOP_USER=hadoop
readonly HADOOP_GROUP=hadoop

# Hadoop client username on Compute Engine Cluster
readonly HDP_USER=hdpuser

# Directory on master where packages are installed
readonly HDP_USER_HOME=/home/hdpuser
readonly MASTER_INSTALL_DIR=/home/hdpuser

# End: edit these values to set up your cluster


# Begin: constants used througout the solution

# Subdirectory where packages files (tar.gz) are stored
readonly PACKAGES_DIR=packages

# Subdirectory where scripts are stored
readonly SCRIPTS_DIR=scripts

# Subdirectory on master where we pull down package files
readonly MASTER_PACKAGE_DIR=/tmp/hdp_tools

# User tmp dir in HDFS
readonly HDFS_TMP_DIR="/tmp"

# Hadoop temp dir (hadoop.tmp.dir)
readonly HADOOP_TMP_DIR="/hadoop/tmp"

# End: constants used througout the solution
