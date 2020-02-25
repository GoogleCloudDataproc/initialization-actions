#!/bin/bash

set -euxo pipefail

# drill installation paths and user & version details
readonly DRILL_USER=drill
readonly DRILL_USER_HOME=/var/lib/drill
readonly DRILL_HOME=/usr/lib/drill
readonly DRILL_LOG_DIR=${DRILL_HOME}/log
readonly DRILL_VERSION='1.15.0'

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function print_err_logs() {
  for i in ${DRILL_LOG_DIR}/*; do
    echo ">>> $i"
    cat "$i"
  done
  return 1
}

function create_hive_storage_plugin() {
  # Create the hive storage plugin
  cat >/tmp/hive_plugin.json <<EOF
{
"name": "hive",
"config": {
    "type": "hive",
    "enabled": true,
    "configProps": {
    "lazyInit": true,
    "hive.metastore.uris": "${hivemeta}",
    "hive.metastore.sasl.enabled": "false",
    "fs.default.name": "${hdfs}"
    }
  }
}
EOF
  curl -d@/tmp/hive_plugin.json -H 'Content-Type: application/json' -X POST http://localhost:8047/storage/hive.json
}

function create_gcs_storage_plugin() {
  # Create GCS storage plugin
  cat >/tmp/gcs_plugin.json <<EOF
{
    "config": {
        "connection": "${gs_plugin_bucket}",
        "enabled": true,
        "formats": {
            "avro": {
                "type": "avro"
            },
            "csv": {
                "delimiter": ",",
                "extensions": [
                    "csv"
                ],
                "type": "text"
            },
            "csvh": {
                "delimiter": ",",
                "extensions": [
                    "csvh"
                ],
                "extractHeader": true,
                "type": "text"
            },
            "json": {
                "extensions": [
                    "json"
                ],
                "type": "json"
            },
            "parquet": {
                "type": "parquet"
            },
            "psv": {
                "delimiter": "|",
                "extensions": [
                    "tbl"
                ],
                "type": "text"
            },
            "sequencefile": {
                "extensions": [
                    "seq"
                ],
                "type": "sequencefile"
            },
            "tsv": {
                "delimiter": "\t",
                "extensions": [
                    "tsv"
                ],
                "type": "text"
            }
        },
        "type": "file",
        "workspaces": {
            "root": {
                "defaultInputFormat": null,
                "location": "/",
                "writable": true
            }
        }
    },
    "name": "gs"
}
EOF
  curl -d@/tmp/gcs_plugin.json -H 'Content-Type: application/json' -X POST http://localhost:8047/storage/gs.json
}

function create_hdfs_storage_plugin() {
  # Create/Update hdfs storage plugin
  cat >/tmp/hdfs_plugin.json <<EOF
{
    "config": {
        "connection": "${hdfs}",
        "enabled": true,
        "formats": {
            "avro": {
                "type": "avro"
            },
            "csv": {
                "delimiter": ",",
                "extensions": [
                    "csv"
                ],
                "type": "text"
            },
            "csvh": {
                "delimiter": ",",
                "extensions": [
                    "csvh"
                ],
                "extractHeader": true,
                "type": "text"
            },
            "json": {
                "extensions": [
                    "json"
                ],
                "type": "json"
            },
            "parquet": {
                "type": "parquet"
            },
            "psv": {
                "delimiter": "|",
                "extensions": [
                    "tbl"
                ],
                "type": "text"
            },
            "sequencefile": {
                "extensions": [
                    "seq"
                ],
                "type": "sequencefile"
            },
            "tsv": {
                "delimiter": "\t",
                "extensions": [
                    "tsv"
                ],
                "type": "text"
            }
        },
        "type": "file",
        "workspaces": {
            "root": {
                "defaultInputFormat": null,
                "location": "/",
                "writable": false
            },
            "tmp": {
                "defaultInputFormat": null,
                "location": "/tmp",
                "writable": true
            }
        }
    },
    "name": "hdfs"
}
EOF
  curl -d@/tmp/hdfs_plugin.json -H 'Content-Type: application/json' -X POST http://localhost:8047/storage/hdfs.json
}

function start_drillbit() {
  # Start drillbit
  sudo -u ${DRILL_USER} ${DRILL_HOME}/bin/drillbit.sh status ||
    sudo -u ${DRILL_USER} ${DRILL_HOME}/bin/drillbit.sh start && sleep 60
  create_hive_storage_plugin
  create_gcs_storage_plugin
  create_hdfs_storage_plugin
}

function main() {
  # Determine the cluster name
  local cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

  # Determine the cluster uuid
  local cluster_uuid=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-uuid)

  # Change these if you have a GCS bucket you'd like to use instead.
  local dataproc_bucket=$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)

  # Use a GCS bucket for Drill profiles, partitioned by cluster name and uuid.
  local profile_store="gs://${dataproc_bucket}/profiles/${cluster_name}/${cluster_uuid}"
  local gs_plugin_bucket="gs://${dataproc_bucket}"

  # intelligently generate the zookeeper string
  readonly zookeeper_cfg="/etc/zookeeper/conf/zoo.cfg"
  readonly zookeeper_client_port=$(grep 'clientPort' ${zookeeper_cfg} |
    tail -n 1 |
    cut -d '=' -f 2)
  readonly zookeeper_list=$(grep '^server\.' ${zookeeper_cfg} |
    tac |
    sort -u -t '=' -k1,1 |
    cut -d '=' -f 2 |
    cut -d ':' -f 1 |
    sed "s/$/:${zookeeper_client_port}/" |
    xargs echo |
    sed "s/ /,/g")

  # Get hive metastore thrift and HDFS URIs
  local hivemeta=$(bdconfig get_property_value \
    --configuration_file /etc/hive/conf/hive-site.xml \
    --name hive.metastore.uris 2>/dev/null)
  local hdfs=$(bdconfig get_property_value \
    --configuration_file /etc/hadoop/conf/core-site.xml \
    --name fs.default.name 2>/dev/null)

  # Create drill pseudo-user.
  useradd -r -m -d ${DRILL_USER_HOME} ${DRILL_USER} || echo

  # Create drill home
  mkdir -p ${DRILL_HOME} && chown ${DRILL_USER}:${DRILL_USER} ${DRILL_HOME}

  # Download and unpack Drill as the pseudo-user.
  wget -nv --timeout=30 --tries=5 --retry-connrefused \
    https://archive.apache.org/dist/drill/drill-${DRILL_VERSION}/apache-drill-${DRILL_VERSION}.tar.gz
  tar -xzf apache-drill-${DRILL_VERSION}.tar.gz -C ${DRILL_HOME} --strip 1

  # Replace default configuration with cluster-specific.
  sed -i "s/drillbits1/${cluster_name}/" ${DRILL_HOME}/conf/drill-override.conf
  sed -i "s/localhost:2181/${zookeeper_list}/" ${DRILL_HOME}/conf/drill-override.conf
  # Make the log directory
  mkdir -p ${DRILL_LOG_DIR} && chown ${DRILL_USER}:${DRILL_USER} ${DRILL_LOG_DIR}

  # Symlink drill conf dir to /etc
  mkdir -p /etc/drill && ln -sf ${DRILL_HOME}/conf /etc/drill/

  # Point drill logs to $DRILL_LOG_DIR
  echo DRILL_LOG_DIR=${DRILL_LOG_DIR} >>${DRILL_HOME}/conf/drill-env.sh

  # Link GCS connector to drill 3rdparty jars
  local connector_dir
  if [[ -d /usr/local/share/google/dataproc/lib ]]; then
    connector_dir=/usr/local/share/google/dataproc/lib
  else
    connector_dir=/usr/lib/hadoop/lib
  fi
  ln -sf ${connector_dir}/gcs-connector-*.jar ${DRILL_HOME}/jars/3rdparty

  # Symlink core-site.xml to $DRILL_HOME/conf
  ln -sf /etc/hadoop/conf/core-site.xml /etc/drill/conf

  # Symlink hdfs-site.xml to $DRILL_HOME/conf
  ln -sf /etc/hadoop/conf/hdfs-site.xml /etc/drill/conf

  # Set ZK PStore to use a GCS Bucket
  # Using GCS makes all Drill profiles available from any drillbit, and also
  # persists the profiles past the lifetime of a cluster.
  cat >>${DRILL_HOME}/conf/drill-override.conf <<EOF
drill.exec: { sys.store.provider.zk.blobroot: "${profile_store}" }
EOF
  chown -R drill:drill /etc/drill/conf/*

  chmod +rx /etc/drill/conf/*

  chmod 777 ${DRILL_HOME}/log/

  start_drillbit || err "Failed to start drill"
  # Clean up
  rm -f /tmp/*_plugin.json
}

main || print_err_logs
