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
CLUSTER=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
DATAPROC_BUCKET=gs://$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)

# naively generate the zookeeper string
ZK=$CLUSTER-m:2181,$CLUSTER-w-0:2181,$CLUSTER-w-1:2181

# Create drill pseudo-user.
useradd -r -m -d $DRILL_USER_HOME $DRILL_USER || echo

# Create drill home
mkdir -p $DRILL_HOME && chown $DRILL_USER:$DRILL_USER $DRILL_HOME

# Download and unpack Drill as the pseudo-user.
curl -L http://apache.mirrors.lucidnetworks.net/drill/drill-$DRILL_VERSION/apache-drill-$DRILL_VERSION.tar.gz | sudo -u $DRILL_USER tar --strip-components=1 -C $DRILL_HOME -vxzf -

# Replace default configuration with cluster-specific.
sed -i "s/drillbits1/$CLUSTER/" $DRILL_HOME/conf/drill-override.conf
sed -i "s/localhost:2181/$ZK/" $DRILL_HOME/conf/drill-override.conf

# Make the log directory
mkdir -p $DRILL_LOG_DIR && chown $DRILL_USER:$DRILL_USER $DRILL_LOG_DIR

# Symlink drill conf dir to /etc
mkdir -p /etc/drill && ln -sf $DRILL_HOME/conf /etc/drill/

# Point drill logs to $DRILL_LOG_DIR
echo DRILL_LOG_DIR=$DRILL_LOG_DIR >> $DRILL_HOME/conf/drill-env.sh

# Copy GCS connector to drill jars
ln -sf /usr/lib/hadoop/lib/gcs-connector-1.6.0-hadoop2.jar $DRILL_HOME/jars/3rdparty

# Symlink core-site.xml to $DRILL_HOME/conf
ln -sf /etc/hadoop/conf/core-site.xml /etc/drill/conf

# Set ZK PStore to use a GCS Bucket
# Makes all Drill profiles available from any drillbit
cat >> $DRILL_HOME/conf/drill-override.conf <<EOF
drill.exec: { sys.store.provider.zk.blobroot: "$DATAPROC_BUCKET/pstore/" }
EOF

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
    "hive.metastore.uris": "thrift://$CLUSTER-m:9083",
    "hive.metastore.sasl.enabled": "false",
    "fs.default.name": "hdfs://$CLUSTER-m/"
    }
  }
}
EOF

# Create GCS storage plugin
cat > /tmp/gcs_plugin.json <<EOF
{
    "config": {
        "connection": "$DATAPROC_BUCKET",
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
    "name": "gs"
}
EOF

curl -d@/tmp/hive_plugin.json -H "Content-Type: application/json" -X POST http://localhost:8047/storage/hive.json
curl -d@/tmp/gcs_plugin.json -H "Content-Type: application/json" -X POST http://localhost:8047/storage/gs.json

# Clean up
rm -f /tmp/*_plugin.json

set +x +e
