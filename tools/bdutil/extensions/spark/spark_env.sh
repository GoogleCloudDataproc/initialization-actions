# Copyright 2014 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file contains environment-variable overrides to be used in conjunction
# with bdutil_env.sh in order to deploy a Hadoop + Spark cluster.
# Usage: ./bdutil deploy -e extensions/spark/spark_env.sh

# An enum of [default|standalone|yarn-client|yarn-cluster].
# In standalone mode, Spark runs it's own daemons and job submissions are made
# to the master daemon by default. yarn-client and yarn-cluster both run inside
# YARN containers. default preserves Spark's default.
SPARK_MODE="standalone"

# URIs of tarballs to install.
SCALA_TARBALL_URI='gs://spark-dist/scala-2.10.3.tgz'
SPARK_HADOOP1_TARBALL_URI='gs://spark-dist/spark-1.1.1-bin-hadoop1.tgz'
SPARK_HADOOP2_TARBALL_URI='gs://spark-dist/spark-1.1.1-bin-hadoop2.4.tgz'

# Directory on each VM in which to install each package.
SCALA_INSTALL_DIR='/home/hadoop/scala-install'
SPARK_INSTALL_DIR='/home/hadoop/spark-install'

# Worker memory to provide in spark-env.sh, as a fraction of total physical
# memory. In the event of running Spark on YARN the NODEMANAGER_MEMORY_FRACTION
# in hadoop2_env.sh replaces this.
SPARK_WORKER_MEMORY_FRACTION='0.8'

# Default memory per Spark executor, as a fraction of total physical memory;
# used for default spark-shell if not overridden with a -D option. Can be used
# to accommodate multiple spark-shells on a single cluster, e.g. if this value
# is set to half the value of SPARK_WORKER_MEMORY_FRACTION then two sets of
# executors can run simultaneously. However, in such a case, then at the time
# of starting 'spark-shell' you must specify fewer cores, e.g.:
# SPARK_JAVA_OPTS="-Dspark.cores.max=4" spark-shell
SPARK_EXECUTOR_MEMORY_FRACTION='0.8'

# Max memory to use by the single Spark daemon process on each node; may need to
# increase when using larger clusters. Expressed as a fraction of total physical
# memory.
SPARK_DAEMON_MEMORY_FRACTION='0.15'

# Spark-standalone master UI is on port 8080.
MASTER_UI_PORTS=('8080' ${MASTER_UI_PORTS[@]})

COMMAND_GROUPS+=(
  "install_spark:
     extensions/spark/install_spark.sh
  "
  "start_spark:
     extensions/spark/start_spark.sh
  "
)

# Installation of spark on master and workers; then start_spark only on master.
COMMAND_STEPS+=(
  'install_spark,install_spark'
  'start_spark,*'
)
