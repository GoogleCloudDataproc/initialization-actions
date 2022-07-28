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

readonly ADDITIONAL_INSTANCES_KEY='attributes/additional-cloud-sql-instances'

readonly PROXY_DIR='/var/run/cloud_sql_proxy'
readonly PROXY_BIN='/usr/local/bin/cloud_sql_proxy'
readonly INIT_SCRIPT='/usr/lib/systemd/system/cloud-sql-proxy.service'
readonly PROXY_LOG_DIR='/var/log/cloud-sql-proxy'

# Whether to configure the Hive metastore to point to a Cloud SQL database.
# This is not required for Hive & Spark I/O.
ENABLE_CLOUD_SQL_METASTORE="$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-hive-metastore || echo 'true')"
readonly ENABLE_CLOUD_SQL_METASTORE

# Whether to enable the proxy on workers. This is not necessary for the
# Metastore, but is required for Hive & Spark I/O.
ENABLE_PROXY_ON_WORKERS="$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-proxy-on-workers || echo 'true')"
readonly ENABLE_PROXY_ON_WORKERS

# Whether to use the private IP address of the cloud sql instance.
USE_CLOUD_SQL_PRIVATE_IP="$(/usr/share/google/get_metadata_value attributes/use-cloud-sql-private-ip || echo 'false')"
readonly USE_CLOUD_SQL_PRIVATE_IP

# Database user to use to access metastore.
DB_HIVE_USER="$(/usr/share/google/get_metadata_value attributes/db-hive-user || echo 'hive')"
readonly DB_HIVE_USER

DB_ADMIN_USER="$(/usr/share/google/get_metadata_value attributes/db-admin-user || echo 'root')"
readonly DB_ADMIN_USER

KMS_KEY_URI="$(/usr/share/google/get_metadata_value attributes/kms-key-uri || echo '')"
readonly KMS_KEY_URI

# Database admin user password used to create the metastore database and user.
DB_ADMIN_PASSWORD_URI="$(/usr/share/google/get_metadata_value attributes/db-admin-password-uri || echo '')"
readonly DB_ADMIN_PASSWORD_URI
if [[ -n "${DB_ADMIN_PASSWORD_URI}" ]]; then
  # Decrypt password
  DB_ADMIN_PASSWORD="$(gsutil cat "${DB_ADMIN_PASSWORD_URI}" |
    gcloud kms decrypt \
      --ciphertext-file - \
      --plaintext-file - \
      --key "${KMS_KEY_URI}")"
  readonly DB_ADMIN_PASSWORD
else
  readonly DB_ADMIN_PASSWORD=''
fi

if [[ -z ${DB_ADMIN_PASSWORD} ]]; then
  readonly DB_ADMIN_PASSWORD_PARAMETER='--password='
else
  DB_ADMIN_PASSWORD_PARAMETER="--password=${DB_ADMIN_PASSWORD}"
  readonly DB_ADMIN_PASSWORD_PARAMETER
fi

# Database password used to access metastore.
DB_HIVE_PASSWORD_URI="$(/usr/share/google/get_metadata_value attributes/db-hive-password-uri || echo '')"
readonly DB_HIVE_PASSWORD_URI
if [[ -n "${DB_HIVE_PASSWORD_URI}" ]]; then
  # Decrypt password
  DB_HIVE_PASSWORD="$(gsutil cat "${DB_HIVE_PASSWORD_URI}" |
    gcloud kms decrypt \
      --ciphertext-file - \
      --plaintext-file - \
      --key "${KMS_KEY_URI}")"
  readonly DB_HIVE_PASSWORD
else
  db_hive_pwd=$(bdconfig get_property_value \
    --configuration_file "/etc/hive/conf/hive-site.xml" \
    --name "javax.jdo.option.ConnectionPassword" 2>/dev/null)
  if [[ "${db_hive_pwd}" == "None" ]]; then
    db_hive_pwd="hive-password"
  fi
  readonly DB_HIVE_PASSWORD=${db_hive_pwd}
fi
if [[ -z ${DB_HIVE_PASSWORD} ]]; then
  readonly DB_HIVE_PASSWORD_PARAMETER=''
else
  readonly DB_HIVE_PASSWORD_PARAMETER="-p${DB_HIVE_PASSWORD}"
fi

METASTORE_INSTANCE="$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance || echo '')"
readonly METASTORE_INSTANCE

ADDITIONAL_INSTANCES="$(/usr/share/google/get_metadata_value ${ADDITIONAL_INSTANCES_KEY} || echo '')"
readonly ADDITIONAL_INSTANCES

METASTORE_PROXY_PORT="$(/usr/share/google/get_metadata_value attributes/metastore-proxy-port || echo '')"
if [[ "${METASTORE_INSTANCE}" =~ =tcp:[0-9]+$ ]]; then
  METASTORE_PROXY_PORT="${METASTORE_INSTANCE##*:}"
else
  METASTORE_PROXY_PORT='3306'
fi
readonly METASTORE_PROXY_PORT

# Name of MySQL database to use for the metastore.
# Will be created if it doesn't exist.
METASTORE_DB="$(/usr/share/google/get_metadata_value attributes/hive-metastore-db || echo 'hive_metastore')"
readonly METASTORE_DB

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

KERBEROS_ENABLED=$(is_component_selected 'kerberos' && echo 'true' || echo 'false')
readonly KERBEROS_ENABLED

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

function get_metastore_instance() {
  local metastore_instance="${METASTORE_INSTANCE}"
  if [[ -z "${metastore_instance}" ]]; then
    err 'Must specify hive-metastore-instance VM metadata'
  fi
  if ! [[ "${metastore_instance}" =~ .+:.+:.+ ]]; then
    err 'hive-metastore-instance must be of form project:region:instance'
  fi
  if ! [[ "${metastore_instance}" =~ =tcp:[0-9]+$ ]]; then
    metastore_instance+="=tcp:${METASTORE_PROXY_PORT}"
  fi
  echo "${metastore_instance}"
}

function get_proxy_flags() {
  local proxy_instances_flags=''
  # If a Cloud SQL instance has both public and private IP, use private IP.
  if [[ ${USE_CLOUD_SQL_PRIVATE_IP} == "true" ]]; then
    proxy_instances_flags+=" --ip_address_types=PRIVATE"
  fi
  if [[ ${ENABLE_CLOUD_SQL_METASTORE} == "true" ]]; then
    local metastore_instance
    metastore_instance=$(get_metastore_instance)
    proxy_instances_flags+=" -instances=${metastore_instance}"
  fi

  if [[ -n "${ADDITIONAL_INSTANCES}" ]]; then
    # Pass additional instances straight to the proxy.
    proxy_instances_flags+=" -instances_metadata=instance/${ADDITIONAL_INSTANCES_KEY}"
  fi

  echo "${proxy_instances_flags}"
}

function install_cloud_sql_proxy() {
  echo 'Installing Cloud SQL Proxy ...' >&2
  # Install proxy.
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
  mv cloud_sql_proxy.linux.amd64 ${PROXY_BIN}
  chmod +x ${PROXY_BIN}

  mkdir -p ${PROXY_DIR}
  mkdir -p ${PROXY_LOG_DIR}

  local proxy_flags
  proxy_flags="$(get_proxy_flags)"

  # Validate db_hive_password and escape invalid xml characters if found.
  local db_hive_password_xml_escaped
  db_hive_password_xml_escaped=${DB_HIVE_PASSWORD//&/&amp;}
  db_hive_password_xml_escaped=${db_hive_password_xml_escaped//</&lt;}
  db_hive_password_xml_escaped=${db_hive_password_xml_escaped//>/&gt;}
  db_hive_password_xml_escaped=${db_hive_password_xml_escaped//'"'/&quot;}

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
  ${proxy_flags} >> /var/log/cloud-sql-proxy/cloud-sql-proxy.log 2>&1'

[Install]
WantedBy=multi-user.target
EOF
  chmod a+rw ${INIT_SCRIPT}

  if [[ $ENABLE_CLOUD_SQL_METASTORE == "true" ]]; then
    # Update hive-site.xml
    cat <<EOF >hive-template.xml
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:${METASTORE_PROXY_PORT}/${METASTORE_DB}</value>
    <description>the URL of the MySQL database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${DB_HIVE_USER}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${db_hive_password_xml_escaped}</value>
  </property>
</configuration>
EOF

    bdconfig merge_configurations \
      --configuration_file /etc/hive/conf/hive-site.xml \
      --source_configuration_file hive-template.xml \
      --clobber
  fi

  echo 'Cloud SQL Proxy installation succeeded' >&2
}

function configure_sql_client() {
  echo "Configuring SQL client ..." >&2
  local mysql_conf_dir
  if [[ -d /etc/mysql/conf.d ]]; then
    mysql_conf_dir=/etc/mysql/conf.d
  elif [[ -d /etc/my.cnf.d ]]; then
    mysql_conf_dir=/etc/my.cnf.d
  else
    # Not m-0.
    echo "Creating config dir for MySQL client on ${HOSTNAME}" >&2
    mysql_conf_dir=/etc/mysql/conf.d
    mkdir -p "${mysql_conf_dir}"
    ln -s "${mysql_conf_dir}" /etc/my.cnf.d
  fi
  # Configure MySQL client to talk to metastore
  cat <<EOF >"${mysql_conf_dir}/cloud-sql-proxy.cnf"
[client]
protocol = tcp
port = ${METASTORE_PROXY_PORT}
EOF
  echo "SQL client configured" >&2
}

function initialize_metastore_db() {
  echo 'Initialzing DB for Hive metastore ...' >&2
  # Check if metastore is initialized.
  if ! mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_HIVE_USER}" "${DB_HIVE_PASSWORD_PARAMETER}" -e ''; then
    mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_ADMIN_USER}" "${DB_ADMIN_PASSWORD_PARAMETER}" -e \
      "CREATE USER '${DB_HIVE_USER}' IDENTIFIED BY '${DB_HIVE_PASSWORD}';"
  fi
  if ! mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_HIVE_USER}" "${DB_HIVE_PASSWORD_PARAMETER}" -e "use ${METASTORE_DB}"; then
    # Initialize a Hive metastore DB
    mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_ADMIN_USER}" "${DB_ADMIN_PASSWORD_PARAMETER}" -e \
      "CREATE DATABASE ${METASTORE_DB};
       GRANT ALL PRIVILEGES ON ${METASTORE_DB}.* TO '${DB_HIVE_USER}';"
    /usr/lib/hive/bin/schematool -dbType mysql -initSchema ||
      err 'Failed to set mysql schema.'
  fi
  echo 'DB initialized for Hive metastore' >&2
}


function run_validation() {
  echo 'Validating ...' >&2

  # Check that metastore schema is compatible.
  /usr/lib/hive/bin/schematool -dbType mysql -info ||
    err 'Run /usr/lib/hive/bin/schematool -dbType mysql -upgradeSchemaFrom <schema-version> to upgrade the schema. Note that this may break Hive metastores that depend on the old schema'

  # Validate it's functioning.
  # On newer Dataproc images, we start hive-server2 after init actions are run,
  # so skip this step if hive-server2 isn't already running.
  if (systemctl show -p SubState --value hive-server2 | grep -q running); then
    local hiveserver_uri
    hiveserver_uri=$(get_hiveserver_uri)
    if ! timeout 60s beeline -u "${hiveserver_uri}" -e 'SHOW TABLES;' >&/dev/null; then
      err 'Failed to bring up Cloud SQL Metastore'
    else
      echo 'Cloud SQL Hive Metastore initialization succeeded' >&2
    fi

    # Execute the Hive "reload function" DDL to reflect permanent functions
    # that have already been created in the HiveServer.
    beeline -u "${hiveserver_uri}" -e "reload function;"
    echo "Reloaded permanent functions"
  fi
   echo 'Validated' >&2
}

function install_mysql_cli() {
  if command -v mysql >/dev/null; then
    echo "MySQL CLI is already installed" >&2
    return
  fi

  echo "Installing MySQL CLI ..." >&2
  if command -v apt >/dev/null; then
    apt update && apt install default-mysql-client -y
  elif command -v yum >/dev/null; then
    yum -y update && yum -y install mysql
  fi
  echo "MySQL CLI installed" >&2
}

function stop_mysql_service() {
  # Debian/Ubuntu
  if (systemctl is-enabled --quiet mysql); then
    echo 'Stopping and disabling mysql.service ...' >&2
    systemctl stop mysql
    systemctl disable mysql
    echo 'mysql.service stopped and disabled' >&2
  # CentOS/Rocky
  elif systemctl is-enabled --quiet mysqld; then
    echo 'Stopping and disabling mysqld.service ...' >&2
    systemctl stop mysqld
    systemctl disable mysqld
    echo 'mysqld.service stopped and disabled' >&2
  else
    echo "Service mysql is not enabled"
  fi
}

function stop_hive_services() {
  if (systemctl is-enabled --quiet hive-server2); then
    echo 'Stopping Hive server2 ...' >&2
    systemctl stop hive-server2
    echo 'Hive server2 stopped' >&2  
  else
    echo "Service Hive server2 is not enabled"
  fi

  if (systemctl is-enabled --quiet hive-metastore); then
    echo 'Stopping Hive metastore ...' >&2
    systemctl stop hive-metastore
    echo 'Hive metastore stopped' >&2  
  else
    echo "Service Hive metastore is not enabled"
  fi
}

function start_hive_services() {
  if (systemctl is-enabled --quiet hive-metastore); then
    echo 'Restarting Hive metastore ...' >&2
    # Re-start metastore to pickup config changes.
    systemctl restart hive-metastore ||
      err 'Unable to start hive-metastore service'
    echo 'Hive metastore restarted' >&2
  else
    echo "Service Hive metastore is not enabled"
  fi

  if (systemctl is-enabled --quiet hive-server2); then
    echo 'Restarting Hive server2 ...' >&2
    # Re-start Hive server2 to re-establish Metastore connection.
    systemctl restart hive-server2 ||
      err 'Unable to start hive-server2 service'
    echo 'Hive server2 restarted' >&2
  else
    echo "Service Hive server2 is not enabled"
  fi
}

function start_cloud_sql_proxy() {
  echo 'Starting Cloud SQL proxy ...' >&2
  systemctl enable cloud-sql-proxy
  systemctl start cloud-sql-proxy ||
    err 'Unable to start cloud-sql-proxy service'

  if [[ $ENABLE_CLOUD_SQL_METASTORE == "true" ]]; then
    run_with_retries nc -zv localhost "${METASTORE_PROXY_PORT}"
  fi

  echo 'Cloud SQL Proxy started' >&2
  echo 'Logs can be found in /var/log/cloud-sql-proxy/cloud-sql-proxy.log' >&2
}

function validate() {
  if [[ $ENABLE_CLOUD_SQL_METASTORE != "true" ]] && [[ -z "${ADDITIONAL_INSTANCES}" ]]; then
    err 'No Cloud SQL instances to proxy'
  fi
}

function update_master() {
  if [[ $ENABLE_CLOUD_SQL_METASTORE == "true" ]]; then
    stop_hive_services
    stop_mysql_service
  fi

  install_cloud_sql_proxy
  start_cloud_sql_proxy
  configure_sql_client

  if [[ $ENABLE_CLOUD_SQL_METASTORE == "true" ]]; then
    install_mysql_cli
    initialize_metastore_db
    start_hive_services
    # Make sure that Hive metastore properly configured.
    run_with_retries run_validation
  fi
}

function update_worker() {
  # This part runs on workers. There is no in-cluster MySQL on workers.
  if [[ $ENABLE_PROXY_ON_WORKERS == "true" ]]; then
    install_cloud_sql_proxy
    start_cloud_sql_proxy
  fi
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  validate
  if [[ "${role}" == 'Master' ]]; then
    update_master
  else
    update_worker
  fi

  echo 'All done' >&2
}

main
