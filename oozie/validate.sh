#!/bin/bash

set -euxo pipefail

namenode=$(bdconfig get_property_value --configuration_file /etc/hadoop/conf/core-site.xml --name fs.default.name 2>/dev/null)

# Upload Oozie example to HDFS if it doesn't exist
hdfs_empty=false
hdfs dfs -ls oozie-examples || hdfs_empty=true
if [[ ${hdfs_empty} == true ]]; then
  tar -zxf /usr/share/doc/oozie/oozie-examples.tar.gz
  hdfs dfs -mkdir -p "/user/${USER}/"
  hadoop fs -put examples oozie-examples
fi

# Download Oozie `job.properties` from HDFS
rm -f job.properties
hdfs dfs -get "oozie-examples/apps/map-reduce/job.properties" job.properties


echo "---------------------------------"
echo "Starting validation on ${HOSTNAME}"
sudo -u hdfs hadoop dfsadmin -safemode leave &>/dev/null
oozie admin -sharelibupdate

# Start example Oozie job
job=$(oozie job -config job.properties -run \
  -D "nameNode=${namenode}:8020" -D "jobTracker=${HOSTNAME}:8032" -D examplesRoot=oozie-examples)
job="${job:5:${#job}}"

# Poll Oozie job info until it's not running
for run in {1..30}; do
  sleep 10s
  job_info=$(oozie job -info "${job}")
  if [[ $job_info != *"RUNNING"* ]]; then
    break
  fi
done

if [[ $job_info == *"SUCCEEDED"* ]]; then
  exit 0
fi

echo "Job ${job} did not succeed"
exit 1
