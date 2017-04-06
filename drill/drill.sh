#!/bin/bash
set -x -e

# where we should install drill
DRILL_USER=drill
DRILL_USER_HOME=/var/lib/drill
DRILL_HOME=/usr/lib/drill
DRILL_LOG_DIR=/var/log/drill
DRILL_VERSION="1.9.0"


# Determine the role of this node
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
# Determine the cluster name
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

# Determine the cluster uuid
CLUSTER_UUID=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-uuid)

# Change these if you have a GCS bucket you'd like to use instead.
DATAPROC_BUCKET=$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)

# Use a GCS bucket for Drill profiles, partitioned by cluster name and uuid.
PROFILE_STORE="gs://$DATAPROC_BUCKET/profiles/$CLUSTER_NAME/$CLUSTER_UUID"
GS_PLUGIN_BUCKET="gs://$DATAPROC_BUCKET"

# intelligently generate the zookeeper string
ZOOKEEPER_CLIENT_PORT=$(grep clientPort /etc/zookeeper/conf/zoo.cfg | cut -d '=' -f 2)
ZOOKEEPER_LIST=$(grep "^server\." /etc/zookeeper/conf/zoo.cfg | \
        cut -d '=' -f 2 | cut -d ':' -f 1 | sed "s/$/:${ZOOKEEPER_CLIENT_PORT}/" | \
        xargs echo | sed "s/ /,/g")

# Get hive metastore thrift and HDFS URIs
HIVEMETA=$(bdconfig get_property_value --configuration_file /etc/hive/conf/hive-site.xml --name hive.metastore.uris 2>/dev/null)
HDFS=$(bdconfig get_property_value --configuration_file /etc/hadoop/conf/core-site.xml --name fs.default.name 2>/dev/null)

# Create drill pseudo-user.
useradd -r -m -d $DRILL_USER_HOME $DRILL_USER || echo

# Create drill home
mkdir -p $DRILL_HOME && chown $DRILL_USER:$DRILL_USER $DRILL_HOME

# Download and unpack Drill as the pseudo-user.
curl -L http://apache.mirrors.lucidnetworks.net/drill/drill-$DRILL_VERSION/apache-drill-$DRILL_VERSION.tar.gz | sudo -u $DRILL_USER tar --strip-components=1 -C $DRILL_HOME -vxzf -

# Replace default configuration with cluster-specific.
sed -i "s/drillbits1/$CLUSTER_NAME/" $DRILL_HOME/conf/drill-override.conf
sed -i "s/localhost:2181/$ZOOKEEPER_LIST/" $DRILL_HOME/conf/drill-override.conf

# Make the log directory
mkdir -p $DRILL_LOG_DIR && chown $DRILL_USER:$DRILL_USER $DRILL_LOG_DIR

# Symlink drill conf dir to /etc
mkdir -p /etc/drill && ln -sf $DRILL_HOME/conf /etc/drill/

# Point drill logs to $DRILL_LOG_DIR
echo DRILL_LOG_DIR=$DRILL_LOG_DIR >> $DRILL_HOME/conf/drill-env.sh

# Link GCS connector to drill 3rdparty jars
ln -sf /usr/lib/hadoop/lib/gcs-connector-*.jar $DRILL_HOME/jars/3rdparty

# Symlink core-site.xml to $DRILL_HOME/conf
ln -sf /etc/hadoop/conf/core-site.xml /etc/drill/conf

# Set ZK PStore to use a GCS Bucket
# Using GCS makes all Drill profiles available from any drillbit, and also
# persists the profiles past the lifetime of a cluster.
cat >> $DRILL_HOME/conf/drill-override.conf <<EOF
drill.exec: { sys.store.provider.zk.blobroot: "$PROFILE_STORE" }
EOF

(
# Start drillbit
sudo -u $DRILL_USER $DRILL_HOME/bin/drillbit.sh status ||\
	sudo -u $DRILL_USER $DRILL_HOME/bin/drillbit.sh start && sleep 10

# Create the hive storage plugin
cat > /tmp/hive_plugin.json <<EOF
{
"name": "hive",
"config": {
    "type": "hive",
    "enabled": true,
    "configProps": {
    "hive.metastore.uris": "$HIVEMETA",
    "hive.metastore.sasl.enabled": "false",
    "fs.default.name": "$HDFS"
    }
  }
}
EOF

# Create GCS storage plugin
cat > /tmp/gcs_plugin.json <<EOF
{
    "config": {
        "connection": "$GS_PLUGIN_BUCKET",
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

# Create/Update hdfs storage plugin
cat > /tmp/hdfs_plugin.json <<EOF
{
    "config": {
        "connection": "$HDFS",
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

curl -d@/tmp/hive_plugin.json -H "Content-Type: application/json" -X POST http://localhost:8047/storage/hive.json
curl -d@/tmp/gcs_plugin.json -H "Content-Type: application/json" -X POST http://localhost:8047/storage/gs.json
curl -d@/tmp/hdfs_plugin.json -H "Content-Type: application/json" -X POST http://localhost:8047/storage/hdfs.json
) || for i in /var/log/drill/*; do echo ">>> $i"; cat "$i" ; done

# Clean up
rm -f /tmp/*_plugin.json

set +x +e
