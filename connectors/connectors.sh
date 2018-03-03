#!/bin/bash

# Determine new connector versions:
# 1. List all GSC/BigQuery connector jars.
# 2. Filter (grep) connector binaries, because javadoc/src jars are also listed.
# 3. Extract (cut) version number from connector paths.
# 4. Sort connector version numbers numerically.
# 5. Use last sorted version number as latest connector version number.
LATEST_BIGQUERY_CONNECTOR_VERSION=$(
  gsutil ls gs://hadoop-lib/bigquery/bigquery-connector-*-hadoop2.jar | \
  grep -E "/bigquery-connector-([0-9]+\.)+[0-9]+-hadoop2\.jar" | \
  cut -d'-' -f4 | \
  sort -t'.' -n -k1,1 -k2,2 -k3,3 | \
  tail -n1
)
LATEST_GCS_CONNECTOR_VERSION=$(
  gsutil ls gs://hadoop-lib/gcs/gcs-connector-*-hadoop2.jar | \
  grep -E "/gcs-connector-([0-9]+\.)+[0-9]+-hadoop2\.jar" | \
  cut -d'-' -f4 | \
  sort -t'.' -n -k1,1 -k2,2 -k3,3 | \
  tail -n1
)

LATEST_BIGQUERY_CONNECTOR_PATH=gs://hadoop-lib/bigquery/bigquery-connector-${LATEST_BIGQUERY_CONNECTOR_VERSION}-hadoop2.jar
LATEST_GCS_CONNECTOR_PATH=gs://hadoop-lib/gcs/gcs-connector-${LATEST_GCS_CONNECTOR_VERSION}-hadoop2.jar

VM_CONNECTORS_DIR=/usr/lib/hadoop/lib

# Update BigQuery connector
rm -f ${VM_CONNECTORS_DIR}/bigquery-connector-*
gsutil cp "$LATEST_BIGQUERY_CONNECTOR_PATH" ${VM_CONNECTORS_DIR}/

# Update GCS connector
rm -f ${VM_CONNECTORS_DIR}/gcs-connector-*
gsutil cp "$LATEST_GCS_CONNECTOR_PATH" ${VM_CONNECTORS_DIR}/
