#!/bin/bash

set -euxo pipefail

VM_CONNECTORS_DIR=/usr/lib/hadoop/lib

declare -A GCS_TO_BQ_VERSIONS_MAP
GCS_TO_BQ_VERSIONS_MAP=(
  ["1.7.0"]="0.11.0")

BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-version || true)
GCS_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/gcs-connector-version || true)

validate_version() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version
  case "$name" in
   bigquery) local -a valid_versions=("${GCS_TO_BQ_VERSIONS_MAP[@]}") ;;
   gcs) local -a valid_versions=("${!GCS_TO_BQ_VERSIONS_MAP[@]}") ;;
   *) echo "Error: invalid connector name - '$name'"; return 1;;
  esac
  if [[ $version ]] && [[ ! "${valid_versions[@]}" =~ "$version" ]]; then
    local IFS=, # print versions concatenated by comma
    echo "ERROR: $name-connector version should be one of [${valid_versions[*]}], but was '$version'"
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

if [[ -z $BIGQUERY_CONNECTOR_VERSION ]] && [[ -z $GCS_CONNECTOR_VERSION ]]; then
  echo "ERROR: None of connector versions are specified"
  exit 1
fi

validate_version "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
validate_version "gcs" "$GCS_CONNECTOR_VERSION"

if [[ "$BIGQUERY_CONNECTOR_VERSION" ]] && [[ "$GCS_CONNECTOR_VERSION" ]] \
    && [[ "${GCS_TO_BQ_VERSIONS_MAP[$GCS_CONNECTOR_VERSION]}" != "$BIGQUERY_CONNECTOR_VERSION" ]]; then
  echo "ERROR: bigquery-connector and gcs-connector versions should be from the same release"
  exit 1
fi

rm -f ${VM_CONNECTORS_DIR}/{gcs,bigquery}-connector-*

download_connector "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
download_connector "gcs" "$GCS_CONNECTOR_VERSION"
