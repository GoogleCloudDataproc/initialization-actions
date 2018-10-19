#!/usr/bin/env bash
# This script is used for environment setup for dr-elephant
set -x -e
# Install on master node
[[ "$(/usr/share/google/get_metadata_value attributes/dataproc-role)" == 'Master' ]] || exit 0
touch ~/.bashrc
cat << 'EOF' > ~/.bashrc
export ACTIVATOR_HOME=/usr/lib/activator-dist-1.3.12/activator-dist-1.3.12
export HADOOP_HOME='/usr/lib/hadoop'
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME='/usr/lib/spark'
export SPARK_CONF_DIR='/usr/lib/spark/conf'
export PATH=$PATH:$HADOOP_HOME:$HADOOP_CONF_DIR:$SPARK_HOME:$SPARK_CONF_DIR:$ACTIVATOR_HOME/bin/
EOF

