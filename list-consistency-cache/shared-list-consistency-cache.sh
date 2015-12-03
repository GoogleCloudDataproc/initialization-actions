#!/bin/bash
#
# Copyright 2015 Google Inc. All Rights Reserved.
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

# Initialization action for reconfiguring Dataproc clusters to use a separate
# NFS server for its GCS list-consistency cache, if the shared NFS server has
# been configured appropriately. Any bdutil or Dataproc deployed master node
# co-located in the same zone as the new Dataproc cluster is suitable for
# serving as a shared list-consistency cache server.

# Replace this with the GCE hostname of whatever suitable NFS master you wish
# to share with your new Dataproc cluster.
SHARED_NFS_MASTER='shared-nfs-cache'

# Replace the autofs config's NFS-cache mount point to use the shared master.
CUR_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
sed -i "s/${CUR_MASTER}/${SHARED_NFS_MASTER}/g" \
    /etc/auto.hadoop_gcs_metadata_cache

# Restart autofs for it to pick up the new config.
service autofs restart

# Prime the directory to make autofs mount it.
ls -l /hadoop_gcs_connector_metadata_cache

# Now check that the 'mount' command finds the right mount point with the right
# remote shared master.
AUTOFS_MOUNTED_MASTER=$(mount | \
    grep "/hadoop_gcs_connector_metadata_cache type nfs4" | \
    cut -d ':' -f 1)
if [[ ${AUTOFS_MOUNTED_MASTER} != ${SHARED_NFS_MASTER} ]]; then
  echo "Autofs mounted ${AUTOFS_MOUNTED_MASTER} != ${SHARED_NFS_MASTER}!"
  echo "Mount:"
  mount
  exit 1
fi

# Clean up NFS cacheserving on the master node.
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  # Remove the crontab for cleaning GCS cache.
  rm -f /etc/cron.d/clean-gcs-caches

  # Remove fstab entry for mounting the export directory.
  sed -i "s/\(.*gcs_connector_metadata_cache.*\)/\#\1/" /etc/fstab

  # Remove /etc/exports entry.
  sed -i "s/\(.*gcs_connector_metadata_cache.*\)/\#\1/" /etc/exports

  # Restart the nfs-kernel-server.
  service nfs-kernel-server restart

  # Unmount the export point and delete it.
  umount /export/hadoop_gcs_connector_metadata_cache/
  rm -rf /export/hadoop_gcs_connector_metadata_cache/
fi
