#!/bin/bash
set -x -e

# where we should install drill
DRILL_USER_HOME=/var/lib/drill
DRILL_HOME=/usr/lib/drill
DRILL_LOG_DIR=/var/log/drill
DRILL_VERSION="1.9.0"


# Determine the role of this node
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
# Determine the cluster name
CLUSTER=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

# naively generate the zookeeper string
ZK=$CLUSTER-m:2181,$CLUSTER-w-0:2181,$CLUSTER-w-1:2181

# Create drill pseudo-user.
useradd -r -m -d $DRILL_USER_HOME drill || echo

# Create drill home
mkdir -p $DRILL_HOME && chown drill:drill $DRILL_HOME

# Download and unpack Drill as the pseudo-user.
curl -L http://apache.mirrors.lucidnetworks.net/drill/drill-$DRILL_VERSION/apache-drill-$DRILL_VERSION.tar.gz | sudo -u drill tar --strip-components=1 -C $DRILL_HOME -vxzf -

# Replace default configuration with cluster-specific.
sed -i "s/drillbits1/$CLUSTER/" $DRILL_HOME/conf/drill-override.conf
sed -i "s/localhost:2181/$ZK/" $DRILL_HOME/conf/drill-override.conf

# Make the log directory
mkdir -p $DRILL_LOG_DIR && chown drill:drill $DRILL_LOG_DIR

# Symlink drill conf dir to /etc
mkdir -p /etc/drill && ln -sf $DRILL_HOME/conf /etc/drill/

# Point drill logs to $DRILL_LOG_DIR
echo DRILL_LOG_DIR=$DRILL_LOG_DIR >> $DRILL_HOME/conf/drill-env.sh

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

curl -d@/tmp/hive_plugin.json -H "Content-Type: application/json" -X POST http://localhost:8047/storage/hive.json

# Clean up
rm -f /tmp/drill_plugin.json

set +x +e
