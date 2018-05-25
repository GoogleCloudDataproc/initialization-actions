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
    rm -f "${VM_CONNECTORS_DIR}/${name}-connector-"*

    # download new connector
    local path=gs://hadoop-lib/${name}/${name}-connector-${version}-hadoop2.jar
    gsutil cp "$path" "${VM_CONNECTORS_DIR}/"
  fi
}

if [[ -z $BIGQUERY_CONNECTOR_VERSION ]] && [[ -z $GCS_CONNECTOR_VERSION ]]; then
  echo "ERROR: None of connector versions are specified"
  exit 1
fi

# because connectors from 1.7 branch are not compatible with previous connectors
# versions (they have the same class relocation paths) we need to update both
# of them, even if only one connector version is set
if [[ -z $BIGQUERY_CONNECTOR_VERSION ]]  && [[ $GCS_CONNECTOR_VERSION = "1.7.0" ]]; then
  BIGQUERY_CONNECTOR_VERSION="0.11.0"
fi
if [[ $BIGQUERY_CONNECTOR_VERSION = "0.11.0" ]]  && [[ -z $GCS_CONNECTOR_VERSION ]]; then
  GCS_CONNECTOR_VERSION="1.7.0"
fi

update_connector "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
update_connector "gcs" "$GCS_CONNECTOR_VERSION"

# Restarts Dataproc Agent after successful initialization
# WARNING: this function relies on undocumented and not officially supported Dataproc Agent
# "sentinel" files to determine successful Agent initialization and not guaranteed
# to work in the future. Use at your own risk!
restart_dataptoc_agent() {
  # Because Dataproc Agent should be restarted after initialization, we need to wait until
  # it will create a sentinel file that signals initialization competition (success or failure)
  while [[ ! -f /var/lib/google/dataproc/has_run_before ]]; do
    sleep 1
  done
  # If Dataproc Agent didn't create a sentinel file that signals initialization
  # failure then it means that initialization succeded and it should be restarted
  if [[ ! -f /var/lib/google/dataproc/has_failed_before ]]; then
    service google-dataproc-agent restart
  fi
}
export -f restart_dataptoc_agent

# Schedule asynchronous Dataproc Agent restart so it will use updated connectors.
# It could not be restarted sycnhronously because Dataproc Agent should be restarted
# after its initialization, including init actions execution, has been completed.
bash -c restart_dataptoc_agent & disown
