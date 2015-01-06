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

set -o nounset
set -o errexit

SCRIPT=$(basename $0)
SCRIPTDIR=$(dirname $0)

source $SCRIPTDIR/project_properties.sh
source $SCRIPTDIR/common_utils.sh

readonly HDFS_CMD="sudo -u $HADOOP_USER -i $HADOOP_HOME/bin/hadoop fs"
readonly HDFS_ROOT_USER="$HADOOP_USER"

function hdfs_mkdir () {
  local dir=$1
  local owner=${2:-}
  local permissions=${3:-}

  emit "  Checking directory $dir"
  if ! $HDFS_CMD -test -d $dir 2> /dev/null; then
    emit "    Creating directory $dir"
    $HDFS_CMD -mkdir $dir
  fi

  if [[ -n "$owner" ]]; then
    emit "    Ensuring owner $owner"
    $HDFS_CMD -chown $owner $dir
  fi

  if [[ -n "$permissions" ]]; then
    emit "    Ensuring permissions $permissions"
    $HDFS_CMD -chmod $permissions $dir
  fi
}
readonly -f hdfs_mkdir

emit ""
emit "*** Begin: $SCRIPT running on master $(hostname) ***"

# Ensure that /tmp exists (it should) and is fully accessible
hdfs_mkdir "$HDFS_TMP_DIR" "$HDFS_ROOT_USER" "777"

# Create a hive-specific scratch space in /tmp for the hdpuser
hdfs_mkdir "$HDFS_TMP_DIR/hive-$HDP_USER" "$HDP_USER"

# Create a warehouse directory (hive) for the hdpuser
hdfs_mkdir "/user" "$HDFS_ROOT_USER"
hdfs_mkdir "/user/$HDP_USER" "$HDP_USER"
hdfs_mkdir "/user/$HDP_USER/warehouse" "$HDP_USER"

# Create a mapreduce staging directory for the hdpuser
if [[ "${HADOOP_MAJOR_VERSION}" == "2" ]]; then
  hdfs_mkdir "/hadoop/mapreduce" "$HADOOP_USER" "o+rw"
  hdfs_mkdir "/hadoop/mapreduce/staging" "$HADOOP_USER" "o+rw"
  hdfs_mkdir "/hadoop/mapreduce/staging/history" "$HADOOP_USER" "777"
  hdfs_mkdir "/hadoop/mapreduce/staging/$HDP_USER" "$HDP_USER"
else
  hdfs_mkdir "$HADOOP_TMP_DIR/mapred/staging/$HDP_USER" "$HDP_USER"
fi

emit ""
emit "*** End: $SCRIPT running on master $(hostname) ***"
emit ""

