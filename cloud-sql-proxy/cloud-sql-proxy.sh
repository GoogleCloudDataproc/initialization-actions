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
set -x -e

ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
METASTORE_INSTANCE="$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance || true)"
#METASTORE_INSTANCE="$(/usr/share/google/get_metadata_value attributes/hive-warehouse-uri || true)"
ADDITIONAL_INSTANCES_KEY='attributes/additional-cloud-sql-instances'
ADDITIONAL_INSTANCES="$(/usr/share/google/get_metadata_value ${ADDITIONAL_INSTANCES_KEY} || true)"
METASTORE_DB="$(/usr/share/google/get_metadata_value attributes/hive-metastore-db || true)"

# Defaults

# Whether to configure the Hive metastore to point to a Cloud SQL database.
# This is not required for Hive & Spark I/O.
ENABLE_CLOUD_SQL_METASTORE=1 # 0 -> false

# Whether to enable the proxy on workers. This is not necessary for the
# Metastore, but is required for Hive & Spark I/O.
ENABLE_PROXY_ON_WORKERS=1 # 0 -> false

# Name of CloudSQL instance to use for the metastore. Must already exist.
# Uncomment to hard code an instance. Metadata will still take precedence.
METASTORE_INSTANCE_DEFAULT= # my-project:my-region:my-instance
METASTORE_INSTANCE="${METASTORE_INSTANCE:-${METASTORE_INSTANCE_DEFAULT}}"

# Name of MySQL database to use for the metastore. Will be created if it doesn't
# exist.
METASTORE_DB="${METASTORE_DB:-hive_metastore}"

# MySQL user to use to access metastore.
HIVE_USER='hive'

# MySQL password to use to access metastore.
HIVE_USER_PASSWORD='hive-password'

# MySQL root password used to initialize metastore.
# Empty is the default for new CloudSQL instances.
MYSQL_ROOT_PASSWORD=''

PROXY_DIR='/var/run/cloud_sql_proxy'
PROXY_BIN='/usr/local/bin/cloud_sql_proxy'
INIT_SCRIPT="/usr/lib/systemd/system/cloud-sql-proxy.service"
METASTORE_PROXY_PORT=3306

# Validation
if (( ! ENABLE_CLOUD_SQL_METASTORE )) && [[ -z "${ADDITIONAL_INSTANCES}" ]];
then
  echo 'No Cloud SQL instances to proxy' >&2
  exit 1
fi

PROXY_INSTANCES_FLAGS=''
if (( ENABLE_CLOUD_SQL_METASTORE )); then
  if [[ -z "${METASTORE_INSTANCE}" ]]; then
    echo 'Must specify hive-metastore-instance VM metadata' >&2
    exit 1
  elif ! [[ "${METASTORE_INSTANCE}" =~ .+:.+:.+ ]]; then
    echo 'hive-metastore-instance must be of form project:region:instance' >&2
    exit 1
  elif ! [[ "${METASTORE_INSTANCE}" =~ =tcp:[0-9]+$ ]]; then
    METASTORE_INSTANCE+="=tcp:${METASTORE_PROXY_PORT}"
  else
    METASTORE_PROXY_PORT="${METASTORE_INSTANCE##*:}"
  fi
  PROXY_INSTANCES_FLAGS+=" --instances ${METASTORE_INSTANCE}"
fi

if [[ -n "${ADDITIONAL_INSTANCES}" ]]; then
  # Pass additional instances straight to the proxy.
  PROXY_INSTANCES_FLAGS+=" --instances_metadata=${ADDITIONAL_INSTANCES_KEY}"
fi

# Disable Hive Metastore and MySql Server
if [[ "${ROLE}" == 'Master' ]] && (( ENABLE_CLOUD_SQL_METASTORE )); then
  systemctl stop hive-metastore
  systemctl stop mysql
  systemctl disable mysql
fi

if [[ "${ROLE}" == 'Master' ]] || (( ENABLE_PROXY_ON_WORKERS )); then
  # Install proxy.
  wget -q https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
  mv cloud_sql_proxy.linux.amd64 ${PROXY_BIN}
  chmod +x ${PROXY_BIN}

  mkdir -p ${PROXY_DIR}

  # Install proxy as systemd service for reboot tolerance.
  cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=Google Cloud SQL Proxy
After=local-fs.target network-online.target
After=google.service
Before=shutdown.target

[Service]
Type=simple
ExecStart=${PROXY_BIN} \
  -dir ${PROXY_DIR} \
  ${PROXY_INSTANCES_FLAGS}

[Install]
WantedBy=multi-user.target
EOF
  chmod a+rw ${INIT_SCRIPT}
  systemctl enable cloud-sql-proxy
  systemctl start cloud-sql-proxy

  echo 'Cloud SQL Proxy installation succeeded' >&2

  if (( ENABLE_CLOUD_SQL_METASTORE )); then
    # Update hive-site.xml
    cat << EOF > hive-template.xml
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:${METASTORE_PROXY_PORT}/${METASTORE_DB}</value>
    <description>the URL of the MySQL database</description>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${HIVE_USER}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${HIVE_USER_PASSWORD}</value>
  </property>
</configuration>
EOF

    bdconfig merge_configurations \
        --configuration_file /etc/hive/conf/hive-site.xml \
        --source_configuration_file hive-template.xml \
        --clobber
  fi
fi

if [[ "${ROLE}" == 'Master' ]] && (( ENABLE_CLOUD_SQL_METASTORE )); then
  # Configure mysql client to talk to metastore as root.
  cat << EOF > /etc/mysql/conf.d/cloud-sql-proxy.cnf
[client]
protocol = tcp
port = ${METASTORE_PROXY_PORT}
user = root
password = ${MYSQL_ROOT_PASSWORD}
EOF

  # Check if metastore is initialized.
  if ! mysql -u "${HIVE_USER}" -p"${HIVE_USER_PASSWORD}" -e ''; then
      mysql -e \
        "CREATE USER '${HIVE_USER}' IDENTIFIED BY '${HIVE_USER_PASSWORD}';"
  fi
  if mysql -e "use ${METASTORE_DB}"; then
    # Extract the warehouse URI.
    HIVE_WAREHOURSE_URI=$(mysql -Nse \
      "SELECT DB_LOCATION_URI FROM ${METASTORE_DB}.DBS WHERE NAME = 'default';")
    bdconfig set_property \
        --name 'hive.metastore.warehouse.dir' \
        --value "${HIVE_WAREHOURSE_URI}" \
        --configuration_file /etc/hive/conf/hive-site.xml \
        --clobber
  else
    # Initialize database with current warehouse URI
    mysql -e \
      "CREATE DATABASE ${METASTORE_DB}; \
      GRANT ALL PRIVILEGES ON ${METASTORE_DB}.* TO '${HIVE_USER}';"
    /usr/lib/hive/bin/schematool -dbType mysql -initSchema
  fi

  # Start metastore back up.
  systemctl start hive-metastore

  # Validate it's functioning.
  if ! hive -e 'SHOW TABLES;' >& /dev/null; then
    echo 'Failed to bring up Cloud SQL Metastore' >&2
    exit 1
  fi
  echo 'Cloud SQL Hive Metastore initialization succeeded' >&2
fi
