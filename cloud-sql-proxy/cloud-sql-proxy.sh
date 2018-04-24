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
set -euxo pipefail

# Whether to configure the Hive metastore to point to a Cloud SQL database.
# This is not required for Hive & Spark I/O.
readonly ENABLE_CLOUD_SQL_METASTORE="$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-hive-metastore || 1)"

# Whether to enable the proxy on workers. This is not necessary for the
# Metastore, but is required for Hive & Spark I/O.
readonly ENABLE_PROXY_ON_WORKERS=$(/usr/share/google/get_metadata_value attributes/enable-cloud-sql-proxy-on-workers || 1)

# MySQL user to use to access metastore.
readonly HIVE_USER='hive'

# MySQL password to use to access metastore.
readonly HIVE_USER_PASSWORD='hive-password'

# MySQL root password used to initialize metastore.
# Empty is the default for new CloudSQL instances.
readonly MYSQL_ROOT_PASSWORD=''

readonly PROXY_DIR='/var/run/cloud_sql_proxy'
readonly PROXY_BIN='/usr/local/bin/cloud_sql_proxy'
readonly INIT_SCRIPT='/usr/lib/systemd/system/cloud-sql-proxy.service'
readonly ADDITIONAL_INSTANCES_KEY='attributes/additional-cloud-sql-instances'

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function configure_proxy_flags() {
  if (( ENABLE_CLOUD_SQL_METASTORE )); then
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
    proxy_instances_flags+=" -instances_metadata=${ADDITIONAL_INSTANCES_KEY}"
  fi
}

function install_cloud_sql_proxy() {
  # Install proxy.
  wget -q https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 \
    || err 'Unable to download cloud-sql-proxy binary'
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
  -dir=${PROXY_DIR} \
  ${proxy_instances_flags}

[Install]
WantedBy=multi-user.target
EOF
  chmod a+rw ${INIT_SCRIPT}
  systemctl enable cloud-sql-proxy
  systemctl start cloud-sql-proxy \
    || err 'Unable to start cloud-sql-proxy service'

  echo 'Cloud SQL Proxy installation succeeded' >&2

  if (( ENABLE_CLOUD_SQL_METASTORE )); then
    # Update hive-site.xml
    cat << EOF > hive-template.xml
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:${metastore_proxy_port}/${metastore_db}</value>
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
}


function configure_sql_client(){
  # Configure mysql client to talk to metastore as root.
  cat << EOF > /etc/mysql/conf.d/cloud-sql-proxy.cnf
[client]
protocol = tcp
port = ${metastore_proxy_port}
user = root
password = ${MYSQL_ROOT_PASSWORD}
EOF

  # Check if metastore is initialized.
  if ! mysql -u "${HIVE_USER}" -p"${HIVE_USER_PASSWORD}" -e ''; then
    mysql -e \
      "CREATE USER '${HIVE_USER}' IDENTIFIED BY '${HIVE_USER_PASSWORD}';"
  fi
  if mysql -e "use ${metastore_db}"; then
    # Extract the warehouse URI.
    HIVE_WAREHOURSE_URI=$(mysql -Nse \
      "SELECT DB_LOCATION_URI FROM ${metastore_db}.DBS WHERE NAME = 'default';")
    bdconfig set_property \
      --name 'hive.metastore.warehouse.dir' \
      --value "${HIVE_WAREHOURSE_URI}" \
      --configuration_file /etc/hive/conf/hive-site.xml \
      --clobber
  else
    # Initialize database with current warehouse URI.
    mysql -e \
      "CREATE DATABASE ${metastore_db}; \
      GRANT ALL PRIVILEGES ON ${metastore_db}.* TO '${HIVE_USER}';"
    /usr/lib/hive/bin/schematool -dbType mysql -initSchema \
      || err 'Failed to set mysql schema.'
  fi

  if ( systemctl is-enabled --quiet hive-metastore ); then
    # Start metastore back up.
    systemctl start hive-metastore \
      || err 'Unable to start hive-metastore service'
  else
    echo "Service hive-metastore is not loaded"
  fi

  # Validate it's functioning.
  if ! hive -e 'SHOW TABLES;' >& /dev/null; then
    err 'Failed to bring up Cloud SQL Metastore'
  fi
  echo 'Cloud SQL Hive Metastore initialization succeeded' >&2

}

function main() {

  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  local metastore_instance
  metastore_instance="$(/usr/share/google/get_metadata_value attributes/hive-metastore-instance || true)"
  #metastore_instance="$(/usr/share/google/get_metadata_value attributes/hive-warehouse-uri || true)"

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
  metastore_proxy_port=3306

  # Validation
  if (( ! ENABLE_CLOUD_SQL_METASTORE )) && [[ -z "${additional_instances}" ]]; then
    err 'No Cloud SQL instances to proxy'
  fi

  local proxy_instances_flags
  proxy_instances_flags=''
  configure_proxy_flags

  if [[ "${role}" == 'Master' ]]; then
    # Disable Hive Metastore and MySql Server.
    if (( ENABLE_CLOUD_SQL_METASTORE )); then
      if ( systemctl is-enabled --quiet hive-metastore ); then
        # Stop hive-metastore if it is enabled
        systemctl stop hive-metastore
      else
        echo "Service hive-metastore is not enabled"
      fi
      systemctl stop mysql
      systemctl disable mysql
    fi
    install_cloud_sql_proxy
    if (( ENABLE_CLOUD_SQL_METASTORE )); then
      configure_sql_client
    fi
  else
    # This part run on workers.
    # Run installation on workers when ENABLE_PROXY_ON_WORKERS is set.
    if (( ENABLE_PROXY_ON_WORKERS )); then
      install_cloud_sql_proxy
    fi
  fi

}

main
