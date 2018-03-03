#!/bin/bash

VM_CONNECTORS_DIR=/usr/lib/hadoop/lib

# Returns latest connector version:
# 1. List all GSC/BigQuery connector jars.
# 2. Filter (grep) connector binaries, because javadoc/src jars are also listed.
# 3. Extract (cut) version number from connector paths.
# 4. Sort connector version numbers numerically.
# 5. Use last sorted version number as latest connector version number.
get_latest_connector_version() {
  local name=$1 # connector name: "bigquery" or "gcs"
  local path_prefix=gs://hadoop-lib/${name}/${name}-connector
  gsutil ls "${path_prefix}-*-hadoop2.jar" | \
    grep -E "${path_prefix}-([0-9]+\.)+[0-9]+-hadoop2\.jar" | \
    cut -d'-' -f4 | \
    sort -t'.' -n -k1,1 -k2,2 -k3,3 | \
    tail -n1
}

get_connector_version() {
  local name=$1 # connector name: "bigquery" or "gcs"
  local metadata_key=attributes/${name}-connector-version
  /usr/share/google/get_metadata_value "${metadata_key}" || \
    get_latest_connector_version "${name}"
}

upload_connector() {
  local name=$1 # connector name: "bigquery" or "gcs"
  local version=$(get_connector_version "$name")
  local path=gs://hadoop-lib/${name}/${name}-connector-${version}-hadoop2.jar
  gsutil cp "$path" ${VM_CONNECTORS_DIR}/
}

rm -f ${VM_CONNECTORS_DIR}/{gcs,bigquery}-connector-*

upload_connector "bigquery"
upload_connector "gcs"
