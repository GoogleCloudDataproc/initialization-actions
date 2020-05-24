#!/bin/bash
# Copyright 2016 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This init script installs a cloud-sql-proxy on each node in the cluster, and
# uses that proxy to expose TCP proxies of one or more CloudSQL instances.
# One of these instances is used for the clusters Hive Metastore.

# Do not use "set -x" to avoid printing passwords in clear in the logs
set -euo pipefail

# Whether to configure the Hive metastore to point to a Cloud SQL database.
# This is not required for Hive & Spark I/O.
readonly enable_cloud_sql_metastore="$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-hive-metastore || echo 'true')"

# Whether to enable the proxy on workers. This is not necessary for the
# Metastore, but is required for Hive & Spark I/O.
readonly enable_proxy_on_workers="$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-proxy-on-workers || echo 'true')"

# Whether to use the private IP address of the cloud sql instance.
readonly use_cloud_sql_private_ip="$(/usr/share/google/get_metadata_value attributes/use-cloud-sql-private-ip || echo 'false')"

# Database user to use to access metastore.
readonly db_hive_user="$(/usr/share/google/get_metadata_value attributes/db-hive-user || echo 'hive')"

readonly db_admin_user="$(/usr/share/google/get_metadata_value attributes/db-admin-user || echo 'root')"

readonly kms_key_uri="$(/usr/share/google/get_metadata_value attributes/kms-key-uri)"

# Database admin user password used to create the metastore database and user.
readonly db_admin_password_uri="$(/usr/share/google/get_metadata_value attributes/db-admin-password-uri)"
if [[ -n "${db_admin_password_uri}" ]]; then
  # Decrypt password
  readonly db_admin_password="$(gsutil cat "${db_admin_password_uri}" |
    gcloud kms decrypt \
      --ciphertext-file - \
      --plaintext-file - \
      --key "${kms_key_uri}")"
else
  readonly db_admin_password=''
fi
if [[ -z ${db_admin_password} ]]; then
  readonly db_admin_password_parameter=""
else
  readonly db_admin_password_parameter="-p${db_admin_password}"
fi

# Database password used to access metastore.
readonly db_hive_password_uri="$(/usr/share/google/get_metadata_value attributes/db-hive-password-uri)"
if [[ -n "${db_hive_password_uri}" ]]; then
  # Decrypt password
  readonly db_hive_password="$(gsutil cat "${db_hive_password_uri}" |
    gcloud kms decrypt \
      --ciphertext-file - \
      --plaintext-file - \
      --key "${kms_key_uri}")"
else
  readonly db_hive_password='hive-password'
fi
if [[ -z ${db_hive_password} ]]; then
  readonly db_hive_password_parameter=""
else
  readonly db_hive_password_parameter="-p${db_hive_password}"
fi

readonly PROXY_DIR='/var/run/cloud_sql_proxy'
readonly PROXY_BIN='/usr/local/bin/cloud_sql_proxy'
readonly INIT_SCRIPT='/usr/lib/systemd/system/cloud-sql-proxy.service'
readonly ADDITIONAL_INSTANCES_KEY='attributes/additional-cloud-sql-instances'
readonly PROXY_LOG_DIR='/var/log/cloud-sql-proxy'

# Dataproc master nodes information
readonly DATAPROC_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

function get_java_property() {
  local property_file=$1
  local property_name=$2
  local property_value
  property_value=$(grep "^${property_name}=" "${property_file}" |
    tail -n 1 | cut -d '=' -f 2- | sed -r 's/\\([#!=:])/\1/g')
  echo "${property_value}"
}

function get_dataproc_property() {
  local property_name=$1
  local property_value
  property_value=$(get_java_property \
    /etc/google-dataproc/dataproc.properties "${property_name}")
  echo "${property_value}"
}

function is_component_selected() {
  local component=$1

  local activated_components
  activated_components=$(get_dataproc_property dataproc.components.activate)

  if [[ ${activated_components} == *${component}* ]]; then
    return 0
  fi
  return 1
}

readonly KERBEROS_ENABLED=$(is_component_selected 'kerberos' && echo 'true' || echo 'false')

function get_hive_principal() {
  # Hostname is fully qualified
  local host
  host=$(hostname -f)
  local domain
  domain=$(dnsdomainname)
  # Realm is uppercase domain name
  echo "hive/${host}@${domain^^}"
}

function get_hiveserver_uri() {
  local base_connect_string="jdbc:hive2://localhost:10000"
  if [[ "${KERBEROS_ENABLED}" == 'true' ]]; then
    local hive_principal
    hive_principal=$(get_hive_principal)
    echo "${base_connect_string}/;principal=${hive_principal}"
  else
    echo "${base_connect_string}"
  fi
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

# Helper to run any command with Fibonacci backoff.
# If all retries fail, returns last attempt's exit code.
# Args: "$@" is the command to run.
function run_with_retries() {
  local retry_backoff=(1 1 2 3 5 8 13 21 34 55 89 144)
  local -a cmd=("$@")
  echo "About to run '${cmd[*]}' with retries..."

  for ((i = 0; i < ${#retry_backoff[@]}; i++)); do
    if "${cmd[@]}"; then
      return 0
    fi
    local sleep_time=${retry_backoff[$i]}
    echo "'${cmd[*]}' attempt $((i + 1)) failed! Sleeping ${sleep_time}." >&2
    sleep "${sleep_time}"
  done

  echo "Final attempt of '${cmd[*]}'..."
  # Let any final error propagate all the way out to any error traps.
  "${cmd[@]}"
}

function configure_proxy_flags() {
  # If a cloud sql instance has both public and private IP, use private IP.
  if [[ $use_cloud_sql_private_ip == "true" ]]; then
    proxy_instances_flags+=" --ip_address_types=PRIVATE"
  fi
  if [[ $enable_cloud_sql_metastore == "true" ]]; then
    if [[ -z "${metastore_instance}" ]]; then
      err 'Must specify hive-metastore-instance VM metadata'
    elif ! [[ "${metastore_instance}" =~ .+:.+:.+ ]]; then
      err 'hive-metastore-instance must be of form project:region:instance'
    elif ! [[ "${metastore_instance}" =~ =tcp:[0-9]+$ ]]; then
      metastore_instance+="=tcp:${metastore_proxy_port}"
    else
      metastore_proxy_port="${metastore_instance##*:}"
    fi
    proxy_instances_flags+=" -instances=${metastore_instance}"
  fi

  if [[ -n "${additional_instances}" ]]; then
    # Pass additional instances straight to the proxy.
    proxy_instances_flags+=" -instances_metadata=instance/${ADDITIONAL_INSTANCES_KEY}"
  fi
}

function install_cloud_sql_proxy() {
  # Install proxy.
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
  mv cloud_sql_proxy.linux.amd64 ${PROXY_BIN}
  chmod +x ${PROXY_BIN}

  mkdir -p ${PROXY_DIR}
  mkdir -p ${PROXY_LOG_DIR}
  # Install proxy as systemd service for reboot tolerance.
  cat <<EOF >${INIT_SCRIPT}
[Unit]
Description=Google Cloud SQL Proxy
After=local-fs.target network-online.target
After=google.service
Before=shutdown.target

[Service]
Type=simple
ExecStart=/bin/sh -c '${PROXY_BIN} \
  -dir=${PROXY_DIR} \
  ${proxy_instances_flags} >> /var/log/cloud-sql-proxy/cloud-sql-proxy.log 2>&1'

[Install]
WantedBy=multi-user.target
EOF
  chmod a+rw ${INIT_SCRIPT}
  systemctl enable cloud-sql-proxy
  systemctl start cloud-sql-proxy ||
    err 'Unable to start cloud-sql-proxy service'

  if [[ $enable_cloud_sql_metastore == "true" ]]; then
    run_with_retries nc -zv localhost "${metastore_proxy_port}"
  fi

  echo 'Cloud SQL Proxy installation succeeded' >&2
  echo 'Logs can be found in /var/log/cloud-sql-proxy/cloud-sql-proxy.log' >&2

  if [[ $enable_cloud_sql_metastore == "true" ]]; then
    # Update hive-site.xml
    cat <<EOF >hive-template.xml
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:${metastore_proxy_port}/${metastore_db}</value>
    <description>the URL of the MySQL database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${db_hive_user}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${db_hive_password}</value>
  </property>
</configuration>
EOF

    bdconfig merge_configurations \
      --configuration_file /etc/hive/conf/hive-site.xml \
      --source_configuration_file hive-template.xml \
      --clobber
  fi
}

function configure_sql_client() {
  # Configure mysql client to talk to metastore
  cat <<EOF >/etc/mysql/conf.d/cloud-sql-proxy.cnf
[client]
protocol = tcp
port = ${metastore_proxy_port}
EOF

  # Check if metastore is initialized.
  if ! mysql -u "${db_hive_user}" "${db_hive_password_parameter}" -e ''; then
    mysql -u "${db_admin_user}" "${db_admin_password_parameter}" -e \
      "CREATE USER '${db_hive_user}' IDENTIFIED BY '${db_hive_password}';"
  fi
  if mysql -u "${db_hive_user}" "${db_hive_password_parameter}" -e "use ${metastore_db}"; then
    # Extract the warehouse URI.
    HIVE_WAREHOURSE_URI=$(mysql -u "${db_hive_user}" "${db_hive_password_parameter}" -Nse \
      "SELECT DB_LOCATION_URI FROM ${metastore_db}.DBS WHERE NAME = 'default';")
    bdconfig set_property \
      --name 'hive.metastore.warehouse.dir' \
      --value "${HIVE_WAREHOURSE_URI}" \
      --configuration_file /etc/hive/conf/hive-site.xml \
      --clobber
  else
    # Initialize a database with current warehouse URI.
    mysql -u "${db_admin_user}" "${db_admin_password_parameter}" -e \
      "CREATE DATABASE ${metastore_db};
       GRANT ALL PRIVILEGES ON ${metastore_db}.* TO '${db_hive_user}';"
    /usr/lib/hive/bin/schematool -dbType mysql -initSchema ||
      err 'Failed to set mysql schema.'
  fi

  run_with_retries run_validation
}

function run_validation() {
  if (systemctl is-enabled --quiet hive-metastore); then
    # Start metastore back up.
    systemctl restart hive-metastore ||
      err 'Unable to start hive-metastore service'
  else
    echo "Service hive-metastore is not loaded"
  fi

  # Check that metastore schema is compatible.
  /usr/lib/hive/bin/schematool -dbType mysql -info ||
    err 'Run /usr/lib/hive/bin/schematool -dbType mysql -upgradeSchemaFrom <schema-version> to upgrade the schema. Note that this may break Hive metastores that depend on the old schema'

  # Validate it's functioning.
  local hiveserver_uri
  hiveserver_uri=$(get_hiveserver_uri)
  if ! timeout 60s beeline -u "${hiveserver_uri}" -e 'SHOW TABLES;' >&/dev/null; then
    err 'Failed to bring up Cloud SQL Metastore'
  else
    echo 'Cloud SQL Hive Metastore initialization succeeded' >&2
  fi

}

function configure_hive_warehouse_dir() {
  # Wait for master 0 to create the metastore db if necessary.
  run_with_retries run_validation

  local hiveserver_uri
  hiveserver_uri=$(get_hiveserver_uri)
  HIVE_WAREHOURSE_URI=$(beeline -u "${hiveserver_uri}" -e "describe database default;" |
    sed '4q;d' | cut -d "|" -f4 | tr -d '[:space:]')

  echo "Hive warehouse uri: $HIVE_WAREHOURSE_URI"

  bdconfig set_property \
    --name 'hive.metastore.warehouse.dir' \
    --value "${HIVE_WAREHOURSE_URI}" \
    --configuration_file /etc/hive/conf/hive-site.xml \
    --clobber
  echo "Updated hive warehouse dir"
}

function reload_function() {
  beeline -u "$(get_hiveserver_uri)" -e "reload function;"
  echo "Reloaded permanent functions"
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  local metastore_instance
  metastore_instance="$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance || true)"

  local additional_instances
  additional_instances="$(/usr/share/google/get_metadata_value ${ADDITIONAL_INSTANCES_KEY} || true)"

  local metastore_db
  metastore_db="$(/usr/share/google/get_metadata_value attributes/hive-metastore-db || true)"

  # Name of CloudSQL instance to use for the metastore. Must already exist.
  # Uncomment to hard code an instance. Metadata will still take precedence.
  metastore_instance_default= # my-project:my-region:my-instance
  metastore_instance="${metastore_instance:-${metastore_instance_default}}"

  # Name of MySQL database to use for the metastore. Will be created if
  # it doesn't exist.

  metastore_db="${metastore_db:-hive_metastore}"

  local metastore_proxy_port
  metastore_proxy_port="$(/usr/share/google/get_metadata_value attributes/metastore-proxy-port || echo '3306')"

  # Validation
  if [[ $enable_cloud_sql_metastore != "true" ]] && [[ -z "${additional_instances}" ]]; then
    err 'No Cloud SQL instances to proxy'
  fi

  local proxy_instances_flags
  proxy_instances_flags=''
  configure_proxy_flags

  if [[ "${role}" == 'Master' ]]; then
    # Disable Hive Metastore and MySql Server.
    if [[ $enable_cloud_sql_metastore == "true" ]]; then
      if (systemctl is-enabled --quiet hive-metastore); then
        # Stop hive-metastore if it is enabled
        systemctl stop hive-metastore
      else
        echo "Service hive-metastore is not enabled"
      fi
      if (systemctl is-enabled --quiet mysql); then
        systemctl stop mysql
        systemctl disable mysql
      else
        echo "Service mysql is not enabled"
      fi
    fi
    install_cloud_sql_proxy
    if [[ $enable_cloud_sql_metastore == "true" ]]; then
      if [[ "${HOSTNAME}" == "${DATAPROC_MASTER}" ]]; then
        # Initialize metastore db instance and set hive.metastore.warehouse.dir
        # on master 0.
        configure_sql_client
      else
        # Set hive.metastore.warehouse.dir only on other masters.
        configure_hive_warehouse_dir
      fi
      # Execute the Hive "reload function" DDL to reflect permanent functions
      # that have already been created in the HiveServer.
      reload_function
    fi
  else
    # This part runs on workers.
    # Run installation on workers when enable_proxy_on_workers is set.
    if [[ $enable_proxy_on_workers == "true" ]]; then
      install_cloud_sql_proxy
    fi
  fi

}

main
