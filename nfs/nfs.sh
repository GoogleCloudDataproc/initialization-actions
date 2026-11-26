#!/bin/bash

# Initialization action for setting up an NFS server on the -m node
# and clients on worker nodes.
# Handles both Kerberized and non-Kerberized clusters.

set -euxo pipefail

readonly workdir="/tmp/nfs-init-action"
mkdir -p "${workdir}/complete"

function is_complete() {
  local phase="$1"
  test -f "${workdir}/complete/${phase}"
}

function mark_complete() {
  local phase="$1"
  touch "${workdir}/complete/${phase}"
}

function mark_incomplete() {
  local phase="$1"
  rm -f "${workdir}/complete/${phase}"
}

function get_metadata_attribute() {
  local -r attribute_name="$1"
  local -r default_value="${2:-}"
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

# Check if OS Login is enabled
readonly ENABLE_OSLOGIN=$(get_metadata_attribute 'enable-oslogin' 'FALSE')
if [[ "${ENABLE_OSLOGIN^^}" != "TRUE" ]]; then
  echo "ERROR: OS Login is not enabled for this cluster (enable-oslogin != TRUE)."
  echo "This is required for user mapping. Please recreate the cluster with --metadata enable-oslogin=TRUE"
  exit 1
fi

readonly ROLE=$(get_metadata_attribute 'dataproc-role')
IS_M_NODE="false"
if [[ "${ROLE}" == 'Master' ]]; then
  IS_M_NODE="true"
fi
readonly IS_M_NODE

# Get the hostname of the primary -m node
readonly PRIMARY_M_NODE_HOSTNAME=$(get_metadata_attribute 'dataproc-master')

# Metadata configurable settings
readonly NFS_EXPORT_DIR=$(get_metadata_attribute 'nfs-export-dir' '/srv/nfs')
readonly NFS_MOUNT_DIR=$(get_metadata_attribute 'nfs-mount-dir' '/srv/nfs')
readonly STATD_PORT=$(get_metadata_attribute 'nfs-statd-port' '32765')
readonly MOUNTD_PORT=$(get_metadata_attribute 'nfs-mountd-port' '32767')
readonly NFS_EXPORT_OPTIONS=$(get_metadata_attribute 'nfs-export-options' 'rw,sync,no_subtree_check')
readonly NFS_CLIENT_OPTIONS=$(get_metadata_attribute 'nfs-client-options' 'nfsvers=4,rw,hard,intr,timeo=600,retrans=3')
readonly KERBEROS_CONFIG_FILE=$(get_metadata_attribute 'nfs-kerberos-config-file' '/etc/krb5.conf')

KERBEROS_ENABLED="false"
if [[ -f "${KERBEROS_CONFIG_FILE}" ]]; then
  KERBEROS_ENABLED="true"
  readonly REALM=$(grep "default_realm = " "${KERBEROS_CONFIG_FILE}" | awk '{print $3}')
fi
readonly KERBEROS_ENABLED

function install_packages() {
  # mark_incomplete install_packages # Optionally force re-run
  if is_complete install_packages; then
    # Even if complete, let's double check nfs-kernel-server on M node
    if [[ "${IS_M_NODE}" == "true" ]]; then
      if ! systemctl list-unit-files | grep -q nfs-server.service && ! systemctl list-unit-files | grep -q nfs-kernel-server.service; then
        echo "WARN: nfs-server service not found, forcing reinstall."
        mark_incomplete install_packages
      elif ! command -v /usr/sbin/exportfs > /dev/null 2>&1; then
         echo "WARN: exportfs command not found, forcing reinstall."
         mark_incomplete install_packages
      fi
    fi
  fi

  if ! is_complete install_packages; then
    echo "Attempting to install/reinstall NFS packages..."
    apt-get update -qq

    if [[ "${IS_M_NODE}" == "true" ]]; then
      echo "Purging nfs-kernel-server to ensure clean install..."
      apt-get purge -y -qq nfs-kernel-server || echo "Purge failed, continuing..."
      apt-get autoremove -y -qq
    fi

    local common_packages=(nfs-common rpcbind)
    local m_node_packages=()
    if [[ "${IS_M_NODE}" == "true" ]]; then
      m_node_packages+=(nfs-kernel-server)
      if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
        m_node_packages+=(krb5-user)
      fi
    fi

    local packages_to_install=("${common_packages[@]}")
    if [[ "${IS_M_NODE}" == "true" ]]; then
      packages_to_install+=("${m_node_packages[@]}")
    fi

    echo "Installing: ${packages_to_install[*]}"
    if ! apt-get install -y -qq --allow-downgrades "${packages_to_install[@]}"; then
      echo "ERROR: Failed to install packages: ${packages_to_install[*]}"
      exit 1
    fi
    echo "Packages installed successfully."
    mark_complete install_packages
  else
    echo "All required NFS packages seem to be installed."
  fi
}


function create_nfs_kerberos_principal() {
  if is_complete create_nfs_kerberos_principal; then echo "NFS principal already created."; return; fi
  local nfs_fqdn
  nfs_fqdn=$(hostname -f) # Use FQDN for the principal
  # Ensure REALM is in uppercase as is convention
  local KRB_REALM
  KRB_REALM=$(echo "${REALM}" | tr 'a-z' 'A-Z')
  local nfs_principal="nfs/${nfs_fqdn}@${KRB_REALM}"
  local keytab_file="/etc/krb5.keytab"

  echo "Using REALM: ${KRB_REALM}"
  echo "Ensuring Kerberos principal: ${nfs_principal}"

  # Attempt to remove any existing principal to ensure clean state
  echo "Attempting to delete any existing principal: ${nfs_principal}"
  kadmin.local -q "delete_principal force ${nfs_principal}" || echo "Principal ${nfs_principal} did not exist or delprinc failed."

  # Create the principal
  echo "Creating Kerberos principal: ${nfs_principal}"
  if ! kadmin.local -q "addprinc -randkey ${nfs_principal}"; then
      echo "ERROR: Failed to create principal ${nfs_principal}"
      exit 1
  fi
  echo "Principal ${nfs_principal} freshly created."

  # Add to keytab
  echo "Adding ${nfs_principal} to ${keytab_file} (Verbose)"
  if ! kadmin.local ktadd -k ${keytab_file} ${nfs_principal}; then
     echo "ERROR: Failed to add principal to keytab ${keytab_file}"
     exit 1
  fi
  chmod 600 "${keytab_file}"
  echo "Keytab ${keytab_file} updated for ${nfs_principal}."
  mark_complete create_nfs_kerberos_principal
}

function provision_kerberos_users() {
  if [[ "${KERBEROS_ENABLED}" != "true" ]]; then
    echo "Kerberos not enabled, skipping user principal provisioning."
    return
  fi
  if is_complete provision_kerberos_users; then echo "User principals already provisioned."; return; fi

  echo "Provisioning Kerberos user principals..."
  USER_LIST=$(get_metadata_attribute 'nfs-kerberos-users' "")

  if [[ -n "${USER_LIST}" ]]; then
    IFS=',' read -r -a users <<< "${USER_LIST}"
    for user in "${users[@]}"; do
      echo "Processing user: ${user}"
      # Assume OS Login has created the user, or create it if you want to be sure.
      if ! id "${user}" > /dev/null 2>&1; then
         echo "Warning: Linux user ${user} does not exist. OS Login should manage this for home directories."
         # Optionally, create the user: useradd -m "${user}"
         continue
      fi

      FULL_PRINCIPAL="${user}@${REALM}"

      # Check if principal exists
      if ! kadmin.local -q "get_principal ${FULL_PRINCIPAL}" > /dev/null 2>&1; then
        echo "Creating principal: ${FULL_PRINCIPAL}"
        kadmin.local -q "add_principal -randkey ${FULL_PRINCIPAL}"

        # Create keytab in user's home directory
        KEYTAB_DIR="/home/${user}/.krb5"
        KEYTAB_FILE="${KEYTAB_DIR}/${user}.keytab"

        mkdir -p "${KEYTAB_DIR}"
        kadmin.local -q "ktadd -k ${KEYTAB_FILE} ${FULL_PRINCIPAL}"

        if [[ -f "${KEYTAB_FILE}" ]]; then
          chown -R "${user}:${user}" "/home/${user}/.krb5"
          chmod 700 "${KEYTAB_DIR}"
          chmod 600 "${KEYTAB_FILE}"
          echo "Keytab for ${user} created at ${KEYTAB_FILE}"
        else
          echo "Warning: Keytab creation failed for ${user}"
        fi
      else
        echo "Principal ${FULL_PRINCIPAL} already exists."
      fi
    done
  else
    echo "No user list found in metadata attribute 'nfs-kerberos-users'. Skipping user principal creation."
  fi
  mark_complete provision_kerberos_users
}

function configure_nfs_server() {
  if is_complete configure_nfs_server; then echo "NFS server already configured."; return; fi
  echo "Configuring NFS server on -m node $(hostname -f)..."
  mkdir -p "${NFS_EXPORT_DIR}"
  chown nobody:nogroup "${NFS_EXPORT_DIR}"
  chmod 777 "${NFS_EXPORT_DIR}" # Adjust permissions as needed

  local ALLOWED_CLIENTS="*" # IMPROVE: Restrict this to cluster IPs

  local export_options="${NFS_EXPORT_OPTIONS}"
  if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
    export_options+=",sec=krb5p"
  else
    export_options+=",sec=sys"
  fi
  export_options+=",root_squash"

  EXPORT_LINE="${NFS_EXPORT_DIR}   ${ALLOWED_CLIENTS}(${export_options})"

  # Ensure /etc/exports exists and Backup before modifying
  touch /etc/exports
  cp /etc/exports /etc/exports.bak

  # Remove any existing line for the export directory
  sed -i "\|^${NFS_EXPORT_DIR} |d" /etc/exports
  
  # Add the new export line
  echo "Adding export for ${NFS_EXPORT_DIR} to /etc/exports"
  echo "${EXPORT_LINE}" >> /etc/exports

  # Configure static ports for NFS services
  echo "Configuring static ports for NFS..."
  # For statd and gssd
  touch /etc/default/nfs-common
  echo "STATDOPTS=\"-p ${STATD_PORT}\"" > /etc/default/nfs-common
  echo "RPCGSSDOPTS=\"-vvv\"" >> /etc/default/nfs-common # Add verbosity to rpc-gssd

  # For mountd (used by nfs-server scripts)
  touch /etc/default/nfs-kernel-server
  echo "RPCMOUNTDOPTS=\"-p ${MOUNTD_PORT}\"" > /etc/default/nfs-kernel-server
  # Potentially add other nfs-server options here if needed

  # Check /etc/nfs.conf for conflicts
  if [[ -f /etc/nfs.conf ]]; then
    echo "Checking /etc/nfs.conf for port conflicts..."
    if grep -E "statd-port|mountd-port" /etc/nfs.conf; then
      echo "WARNING: Port settings found in /etc/nfs.conf, may override /etc/default/ files."
      # Consider commenting them out here if necessary
    fi
  fi

  # --- rpc-statd.service drop-in ---
  mkdir -p /etc/systemd/system/rpc-statd.service.d
  cat << EOF > /etc/systemd/system/rpc-statd.service.d/override.conf
[Service]
EnvironmentFile=-/etc/default/nfs-common
ExecStart=
ExecStart=/sbin/rpc.statd \$STATDOPTS
EOF

  # --- nfs-mountd.service drop-in --- 
  mkdir -p /etc/systemd/system/nfs-mountd.service.d
  cat << EOF > /etc/systemd/system/nfs-mountd.service.d/override.conf
[Service]
EnvironmentFile=-/etc/default/nfs-kernel-server
ExecStart=
ExecStart=/usr/sbin/rpc.mountd \$RPCMOUNTDOPTS
EOF

  # --- nfs-server.service drop-in ---
  mkdir -p /etc/systemd/system/nfs-server.service.d
  cat << EOF > /etc/systemd/system/nfs-server.service.d/override.conf
[Service]
EnvironmentFile=-/etc/default/nfs-common
EnvironmentFile=-/etc/default/nfs-kernel-server
EOF

  # --- rpc-gssd.service drop-in ---
  local gssd_service_name=""
  if systemctl list-unit-files | grep -q rpc-gssd.service; then
    gssd_service_name="rpc-gssd.service"
  elif systemctl list-unit-files | grep -q gssd.service; then
    gssd_service_name="gssd.service"
  fi

  if [[ -n "${gssd_service_name}" ]]; then
    mkdir -p "/etc/systemd/system/${gssd_service_name}.d"
    cat << EOF > "/etc/systemd/system/${gssd_service_name}.d/override.conf"
[Service]
EnvironmentFile=-/etc/default/nfs-common
ExecStart=
ExecStart=/usr/sbin/rpc.gssd \$RPCGSSDOPTS
EOF
  fi

  if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
    create_nfs_kerberos_principal
  fi

  # MANUALLY APPLY EXPORTS *BEFORE* service restarts
  echo "Applying exports..."
  /usr/sbin/exportfs -ra

  # Reload systemd, then enable and restart services
  echo "Reloading systemd, restarting NFS related services on server..."
  systemctl daemon-reload

  local services_to_manage=(rpcbind nfs-idmapd rpc-statd nfs-mountd nfs-server)
  if [[ -n "${gssd_service_name}" ]]; then
    services_to_manage+=(${gssd_service_name::-8})
  fi

  echo "Ensuring NFS related services are enabled and running..."
  for service in "${services_to_manage[@]}"; do
    echo "Stopping ${service}..."
    systemctl stop "${service}.service" || echo "${service} not running or failed to stop."
  done
  sleep 3

  systemctl start rpcbind
  systemctl enable rpcbind || true
  systemctl start nfs-idmapd
  systemctl enable nfs-idmapd || true
  systemctl start rpc-statd
  systemctl enable rpc-statd || true
  systemctl start nfs-mountd
  systemctl enable nfs-mountd || true
  systemctl start nfs-server
  systemctl enable nfs-server || true
  if [[ -n "${gssd_service_name}" ]]; then
    systemctl start "${gssd_service_name}"
    systemctl enable "${gssd_service_name}" || true
  fi

  # Final check
  /usr/sbin/exportfs -v
  echo "NFS server configuration complete."
  mark_complete configure_nfs_server
}

function check_nfs_server_ports() {
  local server_host=$1
  local errors=0
  local ports_to_check=(111 2049 "${MOUNTD_PORT}" "${STATD_PORT}")
  for port in "${ports_to_check[@]}"; do
    if ! nc -vz -w 5 "${server_host}" "${port}" > /dev/null 2>&1; then
      echo "WARN: Port ${port} on ${server_host} is not reachable."
      errors=$((errors + 1))
    fi
  done
  return ${errors}
}

function configure_nfs_client() {
  echo "Configuring NFS client on node $(hostname -f)..."

  # Wait for NFS server ports to be up
  local max_retries=12
  local retry_count=0
  echo "Checking if NFS server ${PRIMARY_M_NODE_HOSTNAME} is ready..."
  while [[ ${retry_count} -lt ${max_retries} ]]; do
    if check_nfs_server_ports "${PRIMARY_M_NODE_HOSTNAME}"; then
      echo "NFS server ports appear to be up."
      break
    fi
    retry_count=$((retry_count + 1))
    if [[ ${retry_count} -lt ${max_retries} ]]; then
      echo "NFS server not fully ready, retrying in 10 seconds... (${retry_count}/${max_retries})"
      sleep 10
    fi
  done

  if [[ ${retry_count} -ge ${max_retries} ]]; then
    echo "ERROR: NFS server ${PRIMARY_M_NODE_HOSTNAME} did not become ready after ${max_retries} attempts."
    exit 1
  fi

  local base_mount_options="${NFS_CLIENT_OPTIONS}"
  local security_option="sec=sys"
  if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
    security_option="sec=krb5p"
  fi

  # Build options array
  local -a fstab_options_array=(
    "noauto"
    "x-systemd.automount"
    "x-systemd.idle-timeout=600"
    "_netdev"
  )
  # Add options from metadata
  IFS=',' read -r -a base_opts_array <<< "${base_mount_options}"
  fstab_options_array+=("${base_opts_array[@]}")
  # Add security option
  fstab_options_array+=("${security_option}")

  # Join array elements with commas
  local fstab_options
  fstab_options=$(IFS=,; echo "${fstab_options_array[*]}")

  if is_complete configure_nfs_client; then echo "NFS client already configured and fstab entry exists."; return; fi

  if [[ ! -d "${NFS_MOUNT_DIR}" ]]; then
    mkdir -p "${NFS_MOUNT_DIR}"
    echo "Created NFS mount directory ${NFS_MOUNT_DIR}"
  fi

  FSTAB_LINE="${PRIMARY_M_NODE_HOSTNAME}:${NFS_EXPORT_DIR}   ${NFS_MOUNT_DIR}   nfs   ${fstab_options}   0   0"

  # Remove any existing line for the mount point
  if grep -q " ${NFS_MOUNT_DIR} " /etc/fstab; then
    echo "Removing existing fstab entry for ${NFS_MOUNT_DIR}"
    sed -i "\|^.* ${NFS_MOUNT_DIR} .*|d" /etc/fstab
  fi

  # Add the new fstab line
  echo "Adding NFS automount to /etc/fstab"
  echo "${FSTAB_LINE}" >> /etc/fstab

  # Reload systemd to recognize new fstab entry
  systemctl daemon-reload

  # Ensure client-side services are running
  systemctl restart rpcbind
  systemctl restart rpc-statd.service
  if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
    # Override rpc-gssd to not require keytab on clients
    local gssd_service_name="rpc-gssd.service"
    # Adjust if gssd.service is used instead on some distros
    if ! systemctl list-unit-files | grep -q rpc-gssd.service; then
        if systemctl list-unit-files | grep -q gssd.service; then
             gssd_service_name="gssd.service"
        else
            echo "WARNING: Neither rpc-gssd.service nor gssd.service found."
            gssd_service_name=""
        fi
    fi

    if [[ -n "${gssd_service_name}" ]]; then
      mkdir -p "/etc/systemd/system/${gssd_service_name}.d"
      cat << EOF > "/etc/systemd/system/${gssd_service_name}.d/client-override.conf"
[Unit]
ConditionPathExists=
EOF
      echo "Created drop-in for ${gssd_service_name} on client."
    fi

    systemctl daemon-reload
    systemctl enable ${gssd_service_name} || true
    systemctl restart ${gssd_service_name}
    if ! systemctl is-active --quiet ${gssd_service_name}; then
        echo "ERROR: ${gssd_service_name} failed to start on client."
        exit 1
    fi
  fi

  # Start the automount unit
  systemctl start $(systemd-escape --path "${NFS_MOUNT_DIR}").automount

  echo "NFS client automount configuration complete."

  # Test the mount
  echo "Performing immediate test mount of ${NFS_MOUNT_DIR}... (Attempt $(date))"
  if ! mount "${NFS_MOUNT_DIR}"; then
    echo "ERROR: Initial test mount of ${NFS_MOUNT_DIR} failed."
    exit 1
  fi
  echo "Test mount successful."

  # Unmount to let automounter handle future mounts
  if ! umount "${NFS_MOUNT_DIR}"; then
    echo "WARN: Failed to unmount after test, automounter will take over."
    umount -l "${NFS_MOUNT_DIR}"
  fi
  echo "Test unmount complete."

  mark_complete configure_nfs_client
}

function test_local_nfs_mount() {
  if ! [[ "${IS_M_NODE}" == "true" ]]; then
    echo "Skipping local NFS mount test (not on -m node)."
    return 0
  fi
  if is_complete test_local_nfs_mount; then echo "Local NFS mount test already passed."; return; fi

  echo "Performing local NFS mount test on -m node..."
  local test_mount_point="/mnt/nfs_test_$(date +%s)"
  mkdir -p "${test_mount_point}"

  local mount_sec_option="sec=sys"
  if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
    mount_sec_option="sec=krb5p"
    local nfs_fqdn=$(hostname -f)
    local KRB_REALM=$(echo "${REALM}" | tr 'a-z' 'A-Z')
    local nfs_principal="nfs/${nfs_fqdn}@${KRB_REALM}"

    echo "Getting ticket for ${nfs_principal} from system keytab..."
    if ! kinit -k -t /etc/krb5.keytab "${nfs_principal}"; then
      echo "ERROR: Failed to kinit with system keytab for ${nfs_principal}"
      rmdir "${test_mount_point}"
      exit 1
    fi
    klist
  fi

  local nfs_fqdn=$(hostname -f)
  echo "Attempting to mount ${nfs_fqdn}:${NFS_EXPORT_DIR} to ${test_mount_point} with ${mount_sec_option}"
  if ! mount -t nfs -o "${mount_sec_option}" "${nfs_fqdn}:${NFS_EXPORT_DIR}" "${test_mount_point}"; then
    echo "ERROR: Failed to mount locally with ${mount_sec_option} using FQDN."
    [[ "${KERBEROS_ENABLED}" == "true" ]] && kdestroy
    rmdir "${test_mount_point}"
    exit 1
  fi

  echo "Local mount successful. Testing file creation..."
  if ! touch "${test_mount_point}/test_file.txt"; then
    echo "ERROR: Failed to create test file on local mount."
    umount "${test_mount_point}" || umount -l "${test_mount_point}"
    [[ "${KERBEROS_ENABLED}" == "true" ]] && kdestroy
    rmdir "${test_mount_point}"
    exit 1
  fi
  rm "${test_mount_point}/test_file.txt"

  echo "Cleaning up local mount..."
  if ! umount "${test_mount_point}"; then
    echo "WARN: Failed to unmount ${test_mount_point} cleanly."
    umount -l "${test_mount_point}"
  fi
  rmdir "${test_mount_point}"
  [[ "${KERBEROS_ENABLED}" == "true" ]] && kdestroy

  echo "Local NFS mount test PASSED."
  mark_complete test_local_nfs_mount
}

function main() {
  install_packages
  if [[ "${IS_M_NODE}" == "true" ]]; then
    if [[ "${KERBEROS_ENABLED}" == "true" ]]; then
      provision_kerberos_users
    fi
    configure_nfs_server
    test_local_nfs_mount # Always test on -m node
    echo "NFS server configuration on -m node finished."
  else
    # Only configure client on worker nodes
    configure_nfs_client
    echo "NFS client configuration on worker node finished."
  fi
  echo "NFS initialization action finished on $(hostname -f)."
}

main
