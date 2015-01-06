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
# with bdutil_env.sh in order to deploy a single-node Hadoop cluster.
# Usage: ./bdutil deploy -e single_node_env.sh

NUM_WORKERS=1

function evaluate_late_variable_bindings() {
  normalize_boolean 'STRIP_EXTERNAL_MIRRORS'
  normalize_boolean 'ENABLE_HDFS'
  normalize_boolean 'INSTALL_GCS_CONNECTOR'
  normalize_boolean 'INSTALL_BIGQUERY_CONNECTOR'
  normalize_boolean 'INSTALL_DATASTORE_CONNECTOR'
  normalize_boolean 'USE_ATTACHED_PDS'
  normalize_boolean 'CREATE_ATTACHED_PDS_ON_DEPLOY'
  normalize_boolean 'DELETE_ATTACHED_PDS_ON_DELETE'
  normalize_boolean 'VERBOSE_MODE'
  normalize_boolean 'DEBUG_MODE'
  normalize_boolean 'OLD_HOSTNAME_SUFFIXES'
  normalize_boolean 'ENABLE_NFS_GCS_FILE_CACHE'

  # In the case of the single-node cluster, we'll just use the whole PREFIX
  # as the name of the master and worker.
  WORKERS[0]=${PREFIX}
  MASTER_HOSTNAME=${PREFIX}
  WORKER_ATTACHED_PDS[0]="${PREFIX}-pd"
  MASTER_ATTACHED_PD="${PREFIX}-pd"

  # Fully qualified HDFS URI of namenode
  NAMENODE_URI="hdfs://${MASTER_HOSTNAME}:8020/"

  # Host and port of jobtracker
  JOB_TRACKER_URI="${MASTER_HOSTNAME}:9101"

  # GCS directory for deployment-related temporary files.
  local staging_dir_base="gs://${CONFIGBUCKET}/bdutil-staging"
  BDUTIL_GCS_STAGING_DIR="${staging_dir_base}/${MASTER_HOSTNAME}"

  # Since $WORKERS and $MASTER_HOSTNAME both refer to the same single-node
  # VM, we must override COMMAND_STEPS to prevent duplicating steps. We also
  # omit deploy-ssh-worker-setup because there is no need to copy SSH keys to
  # the localhost.
  COMMAND_STEPS=(${COMMAND_STEPS[@]/,*/,*})

}
