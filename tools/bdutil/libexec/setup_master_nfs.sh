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

# Installs NFS packages and exports on the master.

if (( ${INSTALL_GCS_CONNECTOR} )) && \
   (( ${ENABLE_NFS_GCS_FILE_CACHE} )) ; then
  # Set up the GCS_ADMIN user.
  setup_gcs_admin

  readonly NFS_EXPORT_POINT="$(get_nfs_export_point)"
  readonly META_CACHE_DIRECTORY='/hadoop/gcs_connector_metadata_cache'

  mkdir -p "${NFS_EXPORT_POINT}"
  mkdir -p "${META_CACHE_DIRECTORY}"
  chown ${GCS_ADMIN}:${GCS_ADMIN} \
      "${NFS_EXPORT_POINT}" "${META_CACHE_DIRECTORY}"

  if ! grep -e "BDUTIL_FILE_CACHE_BINDING" /etc/fstab ; then
    MOUNT_STRING="${META_CACHE_DIRECTORY} ${NFS_EXPORT_POINT} none bind 0 0"
    MOUNT_STRING="${MOUNT_STRING} #BDUTIL_FILE_CACHE_BINDING"
    echo "${MOUNT_STRING}" >> /etc/fstab
    mount "${NFS_EXPORT_POINT}"
  fi

  install_application "nfs-kernel-server" "nfs-utils"

  if ! grep -e "BDUTIL_HADOOP_EXPORT" /etc/exports ; then
    if ! id -u ${GCS_ADMIN}; then
      echo "ERROR: no '${GCS_ADMIN}' user for anonuid in setup_master_nfs.sh"
      exit 1
    fi
    readonly GCSADMIN_UID="$(id -u ${GCS_ADMIN})"
    readonly GCSADMIN_GID="$(id -g ${GCS_ADMIN})"
    OPTIONS_STRING="(rw,all_squash,anonuid=${GCSADMIN_UID}"
    OPTIONS_STRING="${OPTIONS_STRING},anongid=${GCSADMIN_GID})"
    echo "${NFS_EXPORT_POINT} *${OPTIONS_STRING} #BDUTIL_HADOOP_EXPORT" >> \
        /etc/exports
  fi

  if [[ -f /usr/lib/systemd/system/nfs-server.service ]] \
      && which systemctl; then
    # Centos 7
    SERVICE='nfs-server'
    systemctl enable ${SERVICE}
  elif [[ -x /etc/init.d/nfs ]] \
      && [[ -x /etc/init.d/rpcbind ]] \
      && which chkconfig; then
    # Centos 6
    SERVICE='nfs'
    chkconfig --level 235 "${SERVICE}" on
    service rpcbind start
  elif [[ -x /etc/init.d/nfs-kernel-server ]] && which update-rc.d; then
    # Debian 7
    # Installation of the nfs-kernel-server package on Debian-based systems
    # enables the service in default runlevels. As a result, there's no
    # need to run update-rc.d.
    SERVICE='nfs-kernel-server'
  else
    echo 'Cannot find a boot process configuration tool to enable nfs.' >&2
    exit 1
  fi

  #Enable NFS server, load kernel modules and mount /proc/nfsd:
  service "${SERVICE}" start
  service "${SERVICE}" stop

  # Lower grace times for lock re-acquisition post-NFS-restart.
  # This setting is not sticky across boot.
  run_with_retries \
      overwrite_file_with_strings '/proc/sys/fs/nfs/nlm_grace_period' '10'
  run_with_retries \
      overwrite_file_with_strings '/proc/fs/nfsd/nfsv4gracetime' '10'
  run_with_retries \
      overwrite_file_with_strings '/proc/fs/nfsd/nfsv4leasetime' '10'

  service "${SERVICE}" start
fi
