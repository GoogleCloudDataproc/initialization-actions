#!/bin/bash

set -euxo pipefail

VM_CONNECTORS_DIR=/usr/lib/hadoop/lib

declare -A GCS_TO_BQ_VERSIONS_MAP
GCS_TO_BQ_VERSIONS_MAP=(
  ["1.7.0"]="0.11.0")

GCS_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/gcs-connector-version || true)

validate_gcs_connector_version() {
  local version=$1 # connector version
  local -a valid_versions=("${!GCS_TO_BQ_VERSIONS_MAP[@]}")
  if [[ $version ]] && [[ ! "${valid_versions[@]}" =~ "$version" ]]; then
    local IFS=, # print versions concatenated by comma
    echo "ERROR: gcs-connector version should be one of [${valid_versions[*]}], but was '$version'"
    return 1
  fi
}

download_connector() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version
  if [[ $version ]]; then
    local path=gs://hadoop-lib/${name}/${name}-connector-${version}-hadoop2.jar
    gsutil cp "$path" "${VM_CONNECTORS_DIR}/"
  fi
}

if [[ -z $GCS_CONNECTOR_VERSION ]]; then
  echo "ERROR: GCS connector versions not specified"
  exit 1
fi

validate_gcs_connector_version "$GCS_CONNECTOR_VERSION"

BIGQUERY_CONNECTOR_VERSION=${GCS_TO_BQ_VERSIONS_MAP[$GCS_CONNECTOR_VERSION]}

# Update BigQuery connector only if it already exists
if compgen -G "${VM_CONNECTORS_DIR}/bigquery-connector-*" > /dev/null; then
  rm -f ${VM_CONNECTORS_DIR}/bigquery-connector-*
  download_connector "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
fi

rm -f ${VM_CONNECTORS_DIR}/gcs-connector-*
download_connector "gcs" "$GCS_CONNECTOR_VERSION"
