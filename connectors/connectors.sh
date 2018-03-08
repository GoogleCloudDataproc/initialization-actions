#!/bin/bash

set -euxo pipefail

VM_CONNECTORS_DIR=/usr/lib/hadoop/lib

declare -A MIN_CONNECTOR_VERSIONS
MIN_CONNECTOR_VERSIONS=(
  ["bigquery"]="0.11.0"
  ["gcs"]="1.7.0")

BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-version || true)
GCS_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/gcs-connector-version || true)

min_version() {
  echo -e "$1\n$2" | sort -r -t'.' -n -k1,1 -k2,2 -k3,3 | tail -n1
}

validate_version() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version
  local min_valid_version=${MIN_CONNECTOR_VERSIONS[$name]}
  if [[ "$(min_version "$min_valid_version" "$version")" != "$min_valid_version" ]]; then
    echo "ERROR: $name-connector version should be greater than or equal to $min_valid_version, but was $version"
    return 1
  fi
}

update_connector() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version
  if [[ $version ]]; then
    # validate new connector version
    validate_version "$name" "$version"

    # remove old connector
    rm -f ${VM_CONNECTORS_DIR}/${name}-connector-*

    # download new connector
    local path=gs://hadoop-lib/${name}/${name}-connector-${version}-hadoop2.jar
    gsutil cp "$path" "${VM_CONNECTORS_DIR}/"
  fi
}

if [[ -z $BIGQUERY_CONNECTOR_VERSION ]] && [[ -z $GCS_CONNECTOR_VERSION ]]; then
  echo "ERROR: None of connector versions are specified"
  exit 1
fi

update_connector "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
update_connector "gcs" "$GCS_CONNECTOR_VERSION"
