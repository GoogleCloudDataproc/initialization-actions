#!/bin/bash

set -euxo pipefail

readonly VM_CONNECTORS_HADOOP_DIR=/usr/lib/hadoop/lib
readonly VM_CONNECTORS_DATAPROC_DIR=/usr/local/share/google/dataproc/lib

declare -A MIN_CONNECTOR_VERSIONS
MIN_CONNECTOR_VERSIONS=(
  ["bigquery"]="0.11.0"
  ["gcs"]="1.7.0")

# Starting from these versions connectors name changed:
# "...-<version>-hadoop2.jar" -> "...-hadoop2-<version>.jar"
declare -A NEW_NAME_MIN_CONNECTOR_VERSIONS
NEW_NAME_MIN_CONNECTOR_VERSIONS=(
  ["bigquery"]="0.13.5"
  ["gcs"]="1.9.5")

readonly BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-version || true)
readonly GCS_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/gcs-connector-version || true)

readonly BIGQUERY_CONNECTOR_URL=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-url || true)
readonly GCS_CONNECTOR_URL=$(/usr/share/google/get_metadata_value attributes/gcs-connector-url || true)

UPDATED_GCS_CONNECTOR=false

is_worker() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ ${role} != Master ]]; then
    return 0
  fi
  return 1
}

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

function update_connector_url() {
  local -r name=$1
  local -r url=$2

  if [[ $name == gcs ]]; then
    UPDATED_GCS_CONNECTOR=true
  fi

  if [[ -d ${VM_CONNECTORS_DATAPROC_DIR} ]]; then
    local vm_connectors_dir=${VM_CONNECTORS_DATAPROC_DIR}
  else
    local vm_connectors_dir=${VM_CONNECTORS_HADOOP_DIR}
  fi

  # remove old connector
  rm -f "${vm_connectors_dir}/${name}-connector-"*

  gsutil cp "${url}" "${vm_connectors_dir}/"

  local -r jar_name=${url##*/}

  # Update or create version-less connector link
  ln -s -f "${vm_connectors_dir}/${jar_name}" "${vm_connectors_dir}/${name}-connector.jar"
}

update_connector_version() {
  local name=$1    # connector name: "bigquery" or "gcs"
  local version=$2 # connector version

  # validate new connector version
  validate_version "$name" "$version"

  # download new connector
  # connector name could be in one of 2 formats:
  # 1) gs://hadoop-lib/${name}/${name}-connector-hadoop2-${version}.jar
  # 2) gs://hadoop-lib/${name}/${name}-connector-${version}-hadoop2.jar
  local new_name_min_version=${NEW_NAME_MIN_CONNECTOR_VERSIONS[$name]}
  if [[ "$(min_version "$new_name_min_version" "$version")" == "$new_name_min_version" ]]; then
    local jar_name="${name}-connector-hadoop2-${version}.jar"
  else
    local jar_name="${name}-connector-${version}-hadoop2.jar"
  fi

  update_connector_url "${name}" "gs://hadoop-lib/${name}/${jar_name}"
}

if [[ -z $BIGQUERY_CONNECTOR_VERSION && -z $GCS_CONNECTOR_VERSION && -z $BIGQUERY_CONNECTOR_URL && -z $GCS_CONNECTOR_URL ]]; then
  echo "ERROR: None of connector versions or URLs are specified"
  exit 1
fi

# because connectors from 1.7 branch are not compatible with previous connectors
# versions (they have the same class relocation paths) we need to update both
# of them, even if only one connector version is set
if [[ -z $BIGQUERY_CONNECTOR_VERSION ]] && [[ $GCS_CONNECTOR_VERSION == "1.7.0" ]]; then
  BIGQUERY_CONNECTOR_VERSION="0.11.0"
fi
if [[ $BIGQUERY_CONNECTOR_VERSION == "0.11.0" ]] && [[ -z $GCS_CONNECTOR_VERSION ]]; then
  GCS_CONNECTOR_VERSION="1.7.0"
fi

if [[ -n $BIGQUERY_CONNECTOR_VERSION && -n $BIGQUERY_CONNECTOR_URL ]] || [[ -n $GCS_CONNECTOR_VERSION && -n $GCS_CONNECTOR_URL ]]; then
  echo "ERROR: Both, connector version and URL are specified for the same connector"
  exit 1
fi

if [[ -n $BIGQUERY_CONNECTOR_VERSION ]]; then
  update_connector_version "bigquery" "$BIGQUERY_CONNECTOR_VERSION"
fi
if [[ -n $BIGQUERY_CONNECTOR_URL ]]; then
  update_connector_url "bigquery" "$BIGQUERY_CONNECTOR_URL"
fi
if [[ -n $GCS_CONNECTOR_VERSION ]]; then
  update_connector_version "gcs" "$GCS_CONNECTOR_VERSION"
fi
if [[ -n $GCS_CONNECTOR_URL ]]; then
  update_connector_url "gcs" "$GCS_CONNECTOR_URL"
fi

if [[ $UPDATED_GCS_CONNECTOR != true ]]; then
  echo "GCS connector wasn't updated - no need to restart any services"
  exit 0
fi

# Restart YARN NodeManager service on worker nodes so they can pick up updated GCS connector
if is_worker; then
  systemctl kill -s KILL hadoop-yarn-nodemanager
fi

# Restarts Dataproc Agent after successful initialization
# WARNING: this function relies on undocumented and not officially supported Dataproc Agent
# "sentinel" files to determine successful Agent initialization and not guaranteed
# to work in the future. Use at your own risk!
restart_dataproc_agent() {
  # Because Dataproc Agent should be restarted after initialization, we need to wait until
  # it will create a sentinel file that signals initialization competition (success or failure)
  while [[ ! -f /var/lib/google/dataproc/has_run_before ]]; do
    sleep 1
  done
  # If Dataproc Agent didn't create a sentinel file that signals initialization
  # failure then it means that initialization succeded and it should be restarted
  if [[ ! -f /var/lib/google/dataproc/has_failed_before ]]; then
    systemctl kill -s KILL google-dataproc-agent
  fi
}
export -f restart_dataproc_agent

# Schedule asynchronous Dataproc Agent restart so it will use updated connectors.
# It could not be restarted sycnhronously because Dataproc Agent should be restarted
# after its initialization, including init actions execution, has been completed.
bash -c restart_dataproc_agent &
disown
