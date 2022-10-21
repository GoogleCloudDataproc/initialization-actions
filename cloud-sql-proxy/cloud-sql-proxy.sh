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

declare -A DEFAULT_DB_PORT=(['MYSQL']='3306' ['POSTGRES']='5432' ['SQLSERVER']='1433')
declare -A DEFAULT_DB_ADMIN_USER=(['MYSQL']='root' ['POSTGRES']='postgres' ['SQLSERVER']='sqlserver')
declare -A DEFAULT_DB_PROTO=(['MYSQL']='mysql' ['POSTGRES']='postgresql' ['SQLSERVER']='sqlserver')
declare -A DEFAULT_DB_DRIVER=(['MYSQL']='com.mysql.jdbc.Driver' ['POSTGRES']='org.postgresql.Driver' ['SQLSERVER']='com.microsoft.sqlserver.jdbc.SQLServerDriver')

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')] [$(hostname)]: ERROR: $*" >&2
  return 1
}

function log() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')] [$(hostname)]: INFO: $*" >&2
}

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

METASTORE_INSTANCE="$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance || echo '')"
readonly METASTORE_INSTANCE

ADDITIONAL_INSTANCES="$(/usr/share/google/get_metadata_value ${ADDITIONAL_INSTANCES_KEY} || echo '')"
readonly ADDITIONAL_INSTANCES

# Get metastore DB instance type, result be one of MYSQL, POSTGRES, SQLSERVER
function get_cloudsql_instance_type() {
  local instance=$(echo "$1" | cut -d "," -f 1)
  local database=''
  if [[ -z "${instance}" ]]; then
    log 'cloudsql instance VM metadata not specified'
  elif ! [[ "${instance}" =~ .+:.+:.+ ]]; then
    log 'cloudsql instance not of form project:region:instance'
  else
    local project=${instance%*:*:*}
    instance=${instance##*:}
    database=$(gcloud sql instances describe --project=${project} ${instance} | grep 'databaseVersion')
    if [[ -z "${database}" ]]; then
      log 'Unable to describe metastore_instance'
    else
      # Trim off version and whitespaces and use upper case
      # databaseVersion: MYSQL_8_0
      # databaseVersion: POSTGRES_12
      # databaseVersion: SQLSERVER_2019_STANDARD
      database=${database##*:}
      database=${database%%_*}
      database="${database#"${database%%[![:space:]]*}"}"
    fi
  fi
  echo "${database^^}"
}

# CLOUD SQL instance type is one of MYSQL, POSTGRES, SQLSERVER. If not specified
# try to infer it from METASTORE_INSTANCE, ADDITIONAL_INSTANCES, default to MYSQL
CLOUDSQL_INSTANCE_TYPE="$(/usr/share/google/get_metadata_value attributes/cloud-sql-instance-type || echo '')"
CLOUDSQL_INSTANCE_TYPE=${CLOUDSQL_INSTANCE_TYPE^^}
if [[ -z "${CLOUDSQL_INSTANCE_TYPE}" ]]; then
  if [[ -n "${METASTORE_INSTANCE}" ]]; then
    CLOUDSQL_INSTANCE_TYPE=$(get_cloudsql_instance_type "${METASTORE_INSTANCE}")
  elif [[ -n "${ADDITIONAL_INSTANCES}" ]]; then
    CLOUDSQL_INSTANCE_TYPE=$(get_cloudsql_instance_type "${ADDITIONAL_INSTANCES}")
  fi
fi
if [[ -z "${CLOUDSQL_INSTANCE_TYPE}" ]]; then
  CLOUDSQL_INSTANCE_TYPE='MYSQL'
fi
readonly CLOUDSQL_INSTANCE_TYPE

METASTORE_PROXY_PORT="$(/usr/share/google/get_metadata_value attributes/metastore-proxy-port || echo '')"
if [[ "${METASTORE_INSTANCE}" =~ =tcp:[0-9]+$ ]]; then
  METASTORE_PROXY_PORT="${METASTORE_INSTANCE##*:}"
else
  METASTORE_PROXY_PORT=${DEFAULT_DB_PORT["${CLOUDSQL_INSTANCE_TYPE}"]}
fi
readonly METASTORE_PROXY_PORT

# Database user to use to access metastore.
DB_HIVE_USER="$(/usr/share/google/get_metadata_value attributes/db-hive-user || echo 'hive')"
readonly DB_HIVE_USER

DB_ADMIN_USER="$(/usr/share/google/get_metadata_value attributes/db-admin-user || echo '')"
if [[ -z ${DB_ADMIN_USER} ]]; then
  DB_ADMIN_USER=${DEFAULT_DB_ADMIN_USER["${CLOUDSQL_INSTANCE_TYPE}"]}
fi
readonly DB_ADMIN_USER

KMS_KEY_URI="$(/usr/share/google/get_metadata_value attributes/kms-key-uri || echo '')"
readonly KMS_KEY_URI

# Database admin user password used to create the metastore database and user.
DB_ADMIN_PASSWORD_URI="$(/usr/share/google/get_metadata_value attributes/db-admin-password-uri || echo '')"
readonly DB_ADMIN_PASSWORD_URI

DB_ADMIN_PASSWORD=''
if [[ -n "${DB_ADMIN_PASSWORD_URI}" ]]; then
  # Decrypt password
  DB_ADMIN_PASSWORD="$(gsutil cat "${DB_ADMIN_PASSWORD_URI}" |
    gcloud kms decrypt \
      --ciphertext-file - \
      --plaintext-file - \
      --key "${KMS_KEY_URI}")"
fi
if [[ "${CLOUDSQL_INSTANCE_TYPE}" == "POSTGRES" && -z "${DB_ADMIN_PASSWORD}" ]]; then
  log 'POSTGRES DB admin password is not set'
fi
readonly DB_ADMIN_PASSWORD

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

# Helper to run any command with Fibonacci backoff.
# If all retries fail, returns last attempt's exit code.
# Args: "$@" is the command to run.
function run_with_retries() {
  local retry_backoff=(1 1 2 3 5 8 13 21 34 55 89 144)
  local -a cmd=("$@")
  log "About to run '${cmd[*]}' with retries..."

  for ((i = 0; i < ${#retry_backoff[@]}; i++)); do
    if "${cmd[@]}"; then
      return 0
    fi
    local sleep_time=${retry_backoff[$i]}
    log "'${cmd[*]}' attempt $((i + 1)) failed! Sleeping ${sleep_time}."
    sleep "${sleep_time}"
  done

  log "Final attempt of '${cmd[*]}'..."
  # Let any final error propagate all the way out to any error traps.
  "${cmd[@]}"
}

function get_metastore_instance() {
  local metastore_instance="${METASTORE_INSTANCE}"
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
    local db_url=jdbc:${DEFAULT_DB_PROTO["${CLOUDSQL_INSTANCE_TYPE}"]}://localhost:${METASTORE_PROXY_PORT}/${METASTORE_DB}
    local db_driver=${DEFAULT_DB_DRIVER["${CLOUDSQL_INSTANCE_TYPE}"]}

    # Update hive-site.xml
    cat <<EOF >hive-template.xml
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>${db_url}</value>
    <description>the URL of the MySQL database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>${db_driver}</value>
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

  log 'Cloud SQL Proxy installation succeeded'
}

function initialize_mysql_metastore_db() {
  log 'Initialzing MYSQL DB for Hive metastore ...'
  local db_password_param='--password='
  if [[ -n ${DB_ADMIN_PASSWORD} ]]; then
      db_password_param+=${DB_ADMIN_PASSWORD}
  fi
  local db_hive_password_param=''
  if [[ -n ${DB_HIVE_PASSWORD} ]]; then
    db_hive_password_param+="-p${DB_HIVE_PASSWORD}"
  fi

  # Check if metastore is initialized.
  if ! mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_HIVE_USER}" "${db_hive_password_param}" -e ''; then
    mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_ADMIN_USER}" "${db_password_param}" -e \
      "CREATE USER '${DB_HIVE_USER}' IDENTIFIED BY '${DB_HIVE_PASSWORD}';"
  fi
  if ! mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_HIVE_USER}" "${db_hive_password_param}" -e "use ${METASTORE_DB}"; then
    # Initialize a Hive metastore DB
    mysql -h 127.0.0.1 -P "${METASTORE_PROXY_PORT}" -u "${DB_ADMIN_USER}" "${db_password_param}" -e \
      "CREATE DATABASE ${METASTORE_DB};
       GRANT ALL PRIVILEGES ON ${METASTORE_DB}.* TO '${DB_HIVE_USER}';"
    /usr/lib/hive/bin/schematool -dbType mysql -initSchema ||
      err 'Failed to set mysql schema.'
  fi
  log 'MYSQL DB initialized for Hive metastore'
}

function initialize_postgres_metastore_db() {
  log 'Initialzing POSTGRES DB for Hive metastore ...'
  local admin_connection=postgresql://"${DB_ADMIN_USER}":"${DB_ADMIN_PASSWORD}"@127.0.0.1:"${METASTORE_PROXY_PORT}"/
  local hive_connection=postgresql://"${DB_HIVE_USER}":"${DB_HIVE_PASSWORD}"@127.0.0.1:"${METASTORE_PROXY_PORT}"/postgres

  # Check if metastore is initialized.
  if ! psql "${hive_connection}" -c ''; then
    log 'Create DB Hive user...'
    psql "${admin_connection}" -c "CREATE USER ${DB_HIVE_USER} WITH PASSWORD '${DB_HIVE_PASSWORD}';"
  fi
  if ! psql "${hive_connection}" -c '\c "${METASTORE_DB}" ' ; then
    log 'Create Hive Metastore database...'
    psql "${admin_connection}" -c "CREATE DATABASE ${METASTORE_DB};"
    psql "${hive_connection}" -c '\c "${METASTORE_DB}" '
    psql "${admin_connection}" -c "GRANT ALL PRIVILEGES ON DATABASE ${METASTORE_DB} TO ${DB_HIVE_USER} ;"

    log 'Create Hive Metastore schema...'
    /usr/lib/hive/bin/schematool -dbType postgres -initSchema ||
      err 'Failed to set postgres schema.'
  fi
  log 'POSTGRES DB initialized for Hive metastore'
}

function initialize_metastore_db() {
  case ${CLOUDSQL_INSTANCE_TYPE} in
    MYSQL)
      initialize_mysql_metastore_db
      ;;
    POSTGRES)
      initialize_postgres_metastore_db
      ;;
    SQLSERVER)
      # TODO: add SQLSERVER support
      ;;
    *)
      # NO-OP
      ;;
  esac
}

function run_validation() {
  log 'Validating Hive is running...'

  # Check that metastore schema is compatible.
  /usr/lib/hive/bin/schematool -dbType ${CLOUDSQL_INSTANCE_TYPE,,} -info ||
    err 'Run /usr/lib/hive/bin/schematool -dbType ${CLOUDSQL_INSTANCE_TYPE,,} -upgradeSchemaFrom <schema-version> to upgrade the schema. Note that this may break Hive metastores that depend on the old schema'

  # Validate it's functioning.
  # On newer Dataproc images, we start hive-server2 after init actions are run,
  # so skip this step if hive-server2 isn't already running.
  if (systemctl show -p SubState --value hive-server2 | grep -q running); then
    local hiveserver_uri
    hiveserver_uri=$(get_hiveserver_uri)
    if ! timeout 60s beeline -u "${hiveserver_uri}" -e 'SHOW TABLES;' >&/dev/null; then
      err 'Failed to bring up Cloud SQL Metastore'
    else
      log 'Cloud SQL Hive Metastore initialization succeeded'
    fi

    # Execute the Hive "reload function" DDL to reflect permanent functions
    # that have already been created in the HiveServer.
    beeline -u "${hiveserver_uri}" -e "reload function;"
    log 'Reloaded permanent functions'
  fi
   log 'Validated Hive functioning'
}

function install_mysql_cli() {
  if command -v mysql >/dev/null; then
    log "MySQL CLI is already installed"
    return
  fi

  log "Installing MySQL CLI ..."
  if command -v apt >/dev/null; then
    apt update && apt install default-mysql-client -y
  elif command -v yum >/dev/null; then
    yum -y update && yum -y install mysql
  fi
  log "MySQL CLI installed"
}

function install_postgres_cli() {
  if command -v psql >/dev/null; then
    log "POSTGRES CLI is already installed"
    return
  fi

  log "Installing POSTGRES CLI ..."
  if command -v apt >/dev/null; then
    apt update && apt install postgresql-client -y
  elif command -v yum >/dev/null; then
    yum -y update && yum -y install postgresql
  fi
  log "POSTGRES CLI installed"
}

function install_db_cli() {
  case ${CLOUDSQL_INSTANCE_TYPE} in
    MYSQL)
      install_mysql_cli
      ;;
    POSTGRES)
      install_postgres_cli
      ;;
    SQLSERVER)
      # TODO: add SQL support
      err 'Fail fast here if SQLSERVER support is not enabled.'
      ;;
    *)
      # NO-OP
      ;;
  esac
}

function stop_mysql_service() {
  # Debian/Ubuntu
  if (systemctl is-enabled --quiet mysql); then
    log 'Stopping and disabling mysql.service ...'
    systemctl stop mysql
    systemctl disable mysql
    log 'mysql.service stopped and disabled'
  # CentOS/Rocky
  elif systemctl is-enabled --quiet mysqld; then
    log 'Stopping and disabling mysqld.service ...'
    systemctl stop mysqld
    systemctl disable mysqld
    log 'mysqld.service stopped and disabled'
  else
    log 'Service mysql is not enabled'
  fi
}

function stop_hive_services() {
  if (systemctl is-enabled --quiet hive-server2); then
    log 'Stopping Hive server2 ...'
    systemctl stop hive-server2
    log 'Hive server2 stopped'
  else
    echo "Service Hive server2 is not enabled"
  fi

  if (systemctl is-enabled --quiet hive-metastore); then
    log 'Stopping Hive metastore ...'
    systemctl stop hive-metastore
    log 'Hive metastore stopped'
  else
    echo "Service Hive metastore is not enabled"
  fi
}

function start_hive_services() {
  if (systemctl is-enabled --quiet hive-metastore); then
    log 'Restarting Hive metastore ...'
    # Re-start metastore to pickup config changes.
    systemctl restart hive-metastore ||
      err 'Unable to start hive-metastore service'
    log 'Hive metastore restarted'
  else
    echo "Service Hive metastore is not enabled"
  fi

  if (systemctl is-enabled --quiet hive-server2); then
    log 'Restarting Hive server2 ...'
    # Re-start Hive server2 to re-establish Metastore connection.
    systemctl restart hive-server2 ||
      err 'Unable to start hive-server2 service'
    log 'Hive server2 restarted'
  else
    echo "Service Hive server2 is not enabled"
  fi
}

function start_cloud_sql_proxy() {
  log 'Starting Cloud SQL proxy ...'
  systemctl enable cloud-sql-proxy
  systemctl start cloud-sql-proxy ||
    err 'Unable to start cloud-sql-proxy service'

  if [[ $ENABLE_CLOUD_SQL_METASTORE == "true" ]]; then
    run_with_retries nc -zv localhost "${METASTORE_PROXY_PORT}"
  fi

  log 'Cloud SQL Proxy started'
  log 'Logs can be found in /var/log/cloud-sql-proxy/cloud-sql-proxy.log'
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

  if [[ $ENABLE_CLOUD_SQL_METASTORE == "true" ]]; then
    install_db_cli

    # Retry as there may be failures due to race condition
    run_with_retries initialize_metastore_db

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

  log 'All done'
}

main
