#!/usr/bin/env bash
#
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

# Runs some basic "hello world" logic to test parallelization, basic grouping
# functionality, persisting an RDD to the distributed filesystem, viewing the
# same files with "hadoop fs", reading it back in with Spark, and finally
# deleting the files with "hadoop fs".
#
# Usage: ./bdutil shell < spark-validate-setup.sh
#
# Warning: If the script returns a nonzero code, then there may be some test
# files which should be cleaned up; you can find these with
# hadoop fs -ls validate_spark_*

set -e

# Find hadoop-confg.sh
HADOOP_CONFIGURE_CMD=''
HADOOP_CONFIGURE_CMD=$(find ${HADOOP_LIBEXEC_DIR} ${HADOOP_PREFIX} \
    /home/hadoop /usr/*/hadoop* -name hadoop-config.sh | head -n 1)

# If hadoop-config.sh has been found source it
if [[ -n "${HADOOP_CONFIGURE_CMD}" ]]; then
  echo "Sourcing '${HADOOP_CONFIGURE_CMD}'"
  . ${HADOOP_CONFIGURE_CMD}
fi

HADOOP_CMD="${HADOOP_PREFIX}/bin/hadoop"

# Find the spark-shell command.
SPARK_SHELL=$(find /home/hadoop -name spark-shell | head -n 1)

# Create a unique directory for testing RDD persistence.
PARENT_DIR="/validate_spark_$(date +%s)"

# Get info about the cluster.
NUM_WORKERS=$(wc -l $(dirname ${SPARK_SHELL})/../conf/slaves \
    | cut -d ' ' -f 1)
NUM_CPUS=$(grep -c ^processor /proc/cpuinfo)
NUM_SHARDS=$((${NUM_WORKERS} * ${NUM_CPUS}))
echo "NUM_WORKERS: ${NUM_WORKERS}"
echo "NUM_CPUS: ${NUM_CPUS}"
echo "NUM_SHARDS: ${NUM_SHARDS}"

# Create an RDD.
${SPARK_SHELL} << EOF
import java.net.InetAddress
val greetings = sc.parallelize(1 to ${NUM_SHARDS}).map({ i =>
  (i, InetAddress.getLocalHost().getHostName(),
   "Hello " + i + ", from host " + InetAddress.getLocalHost().getHostName())
})

val uniqueHostnames = greetings.map(tuple => tuple._2).distinct()
println("Got unique hostnames:")
for (hostname <- uniqueHostnames.collect()) {
  println(hostname)
}
val uniqueGreetings = greetings.map(tuple => tuple._3).distinct()
println("Unique greetings:")
for (greeting <- uniqueGreetings.collect()) {
  println(greeting)
}

val numHostnames = uniqueHostnames.count()
if (numHostnames != ${NUM_WORKERS}) {
  println("Expected ${NUM_WORKERS} hosts, got " + numHostnames)
  exit(1)
}

val numGreetings = uniqueGreetings.count()
if (numGreetings != ${NUM_SHARDS}) {
  println("Expected ${NUM_SHARDS} greetings, got " + numGreetings)
  exit(1)
}

greetings.saveAsObjectFile("${PARENT_DIR}/")
exit(0)
EOF

# Check it with "hadoop fs".
echo "Checking _SUCCESS marker with 'hadoop fs'..."
NUM_FILES=$(${HADOOP_CMD} fs -ls ${PARENT_DIR}/part-* | wc -l | cut -d ' ' -f 1)
echo "Found ${NUM_FILES} files."
${HADOOP_CMD} fs -stat "${PARENT_DIR}/_SUCCESS"

# Read the RDD back in and verify it.
${SPARK_SHELL} << EOF
val greetings = sc.objectFile[(Int, String, String)]("${PARENT_DIR}/")

val uniqueHostnames = greetings.map(tuple => tuple._2).distinct()
println("Got unique hostnames:")
for (hostname <- uniqueHostnames.collect()) {
  println(hostname)
}
val uniqueGreetings = greetings.map(tuple => tuple._3).distinct()
println("Unique greetings:")
for (greeting <- uniqueGreetings.collect()) {
  println(greeting)
}

val numHostnames = uniqueHostnames.count()
if (numHostnames != ${NUM_WORKERS}) {
  println("Expected ${NUM_WORKERS} hosts, got " + numHostnames)
  exit(1)
}

val numGreetings = uniqueGreetings.count()
if (numGreetings != ${NUM_SHARDS}) {
  println("Expected ${NUM_SHARDS} greetings, got " + numGreetings)
  exit(1)
}
exit(0)
EOF

echo "Cleaning up ${PARENT_DIR}..."
${HADOOP_CMD} fs -rmr ${PARENT_DIR}
echo 'All done!'
