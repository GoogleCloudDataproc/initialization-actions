#!/bin/bash

declare -A MIN_CONNECTOR_VERSIONS
MIN_CONNECTOR_VERSIONS=(
  ["bigquery"]="0.10.1"
  ["gcs"]="1.6.0")

declare -A GCS_TO_BQ_VERSIONS_MAP
GCS_TO_BQ_VERSIONS_MAP=(
  ["1.6.0"]="0.10.1"
  ["1.6.1"]="0.10.2"
  ["1.6.2"]="0.10.3"
  ["1.6.3"]="0.10.4"
  ["1.7.0"]="0.11.0")

min_version() {
  echo -e "$1\n$2" | sort -r -t'.' -n -k1,1 -k2,2 -k3,3 | tail -n1
}

validate_version() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version
  local min_valid_version=${MIN_CONNECTOR_VERSIONS[$name]}
  if [[ "$version" ]] && [[ "$(min_version "$min_valid_version" "$version")" != "$min_valid_version" ]]; then
    echo "ERROR: $name-connector version should be greater than or equal to $min_valid_version, but was $version"
    exit 1
  fi
}

upload_connector() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version
  if [[ $version ]]; then
    local path=gs://hadoop-lib/${name}/${name}-connector-${version}-hadoop2.jar
    gsutil cp "$path" "${VM_CONNECTORS_DIR}/"
  fi
}

VM_CONNECTORS_DIR=/usr/lib/hadoop/lib

BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-version)
GCS_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/gcs-connector-version)

if [[ -z $BIGQUERY_CONNECTOR_VERSION ]] && [[ -z $GCS_CONNECTOR_VERSION ]]; then
  echo "ERROR: None of connector versions are specified"
  exi 1
fi

validate_version "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
validate_version "gcs" "$GCS_CONNECTOR_VERSION"

if [[ "$BIGQUERY_CONNECTOR_VERSION" ]] && [[ "$GCS_CONNECTOR_VERSION" ]] \
    && [[ "${GCS_TO_BQ_VERSIONS_MAP[$GCS_CONNECTOR_VERSION]}" != "$BIGQUERY_CONNECTOR_VERSION" ]]; then
  echo "ERROR: bigquery-connector and gcs-connector versions should be from the same release"
  exit 1
fi

rm -f ${VM_CONNECTORS_DIR}/{gcs,bigquery}-connector-*

upload_connector "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
upload_connector "gcs" "$GCS_CONNECTOR_VERSION"
