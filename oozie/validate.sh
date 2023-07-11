#!/bin/bash

set -euxo pipefail

readonly CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
readonly master_node=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_ADDITIONAL=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)
CLUSTER_HOSTNAME="${CLUSTER_NAME}"
if [[ -z "${MASTER_ADDITIONAL}" ]]; then
  CLUSTER_HOSTNAME+="-m"
fi
readonly CLUSTER_HOSTNAME

# Upload Oozie example to HDFS if it doesn't exist
if ! hdfs dfs -test -d "/user/${USER}/oozie-examples"; then
  tar -xzf /usr/share/doc/oozie/oozie-examples.tar.gz
  hdfs dfs -mkdir -p "/user/${USER}/"
  hadoop fs -put ./examples "/user/${USER}/oozie-examples"
fi

# Download Oozie `job.properties` from HDFS
hdfs dfs -get -f "/user/${USER}/oozie-examples/apps/map-reduce/job.properties" job.properties
sed -i "s/localhost/${CLUSTER_HOSTNAME}/g" job.properties
cat job.properties

echo -e "\nStarting validation on ${HOSTNAME}:"

if [[ -z "${MASTER_ADDITIONAL}" ]]; then
  OOZIE_URL="http://${CLUSTER_HOSTNAME}:11000/oozie"
else
  OOZIE_URL="http://${master_node}:11000/oozie"
fi

oozie admin -oozie ${OOZIE_URL} -sharelibupdate

# Start example Oozie job
job=$(oozie job -oozie ${OOZIE_URL} -config job.properties -run \
  -D "nameNode=hdfs://${CLUSTER_HOSTNAME}:8020" \
  -D "jobTracker=${CLUSTER_HOSTNAME}:8032" \
  -D "resourceManager=${CLUSTER_HOSTNAME}:8032" \
  -D "examplesRoot=oozie-examples")
job="${job:5:${#job}}"

# Poll Oozie job info until it's not running
for _ in {1..20}; do
  job_status=$(oozie job -oozie ${OOZIE_URL} -info "${job}" | grep "^Status")
  if [[ $job_status != *"RUNNING"* ]]; then
    break
  fi
  sleep 10
done

if [[ $job_status == *"SUCCEEDED"* ]]; then
  exit 0
fi

echo "Job ${job} did not succeed:"
oozie job -oozie ${OOZIE_URL} -info "${job}"
exit 1
