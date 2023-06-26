#!/bin/bash

# Installs Hadoop BigQuery and/or Spark BigQuery connectors
# onto a Cloud Dataproc cluster.

set -euxo pipefail

readonly VM_CONNECTORS_HADOOP_DIR=/usr/lib/hadoop/lib
readonly VM_CONNECTORS_DATAPROC_DIR=/usr/local/share/google/dataproc/lib

declare -A MIN_CONNECTOR_VERSIONS
MIN_CONNECTOR_VERSIONS=(
  ["bigquery"]="1.0.0"
  ["spark-bigquery"]="0.17.0"
  ["hive-bigquery"]="0.0.0")

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

readonly BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-version || true)
readonly SPARK_BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/spark-bigquery-connector-version || true)
readonly HIVE_BIGQUERY_CONNECTOR_VERSION=$(/usr/share/google/get_metadata_value attributes/hive-bigquery-connector-version || true)

readonly BIGQUERY_CONNECTOR_URL=$(/usr/share/google/get_metadata_value attributes/bigquery-connector-url || true)
readonly SPARK_BIGQUERY_CONNECTOR_URL=$(/usr/share/google/get_metadata_value attributes/spark-bigquery-connector-url || true)
readonly HIVE_BIGQUERY_CONNECTOR_URL=$(/usr/share/google/get_metadata_value attributes/hive-bigquery-connector-url || true)

is_worker() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ $role != Master ]]; then
    return 0
  fi
  return 1
}

min_version() {
  echo -e "$1\n$2" | sort -r -t'.' -n -k1,1 -k2,2 -k3,3 | tail -n1
}

function compare_versions_lte {
  [ "$1" = "$(echo -e "$1\n$2" | sort -V | head -n1)" ]
}

function compare_versions_lt() {
  [ "$1" = "$2" ] && return 1 || compare_versions_lte $1 $2
}

function get_connector_url() {
  # Hadoop BigQuery connector:
  #   gs://hadoop-lib/bigquery-connector/bigquery-connector-hadoop{hadoop_version}-${version}.jar
  #
  # Spark BigQuery connector:
  #   gs://spark-lib/bigquery/spark-bigquery-connector-with-dependencies_${scala_version}-${version}.jar
  #
  # Hive BigQuery connector:
  #   gs://hadoop-lib/hive-bigquery/hive-bigquery-connector-${version}.jar

  local -r name=$1
  local -r version=$2

  # spark-bigquery
  if [[ $name == spark-bigquery ]]; then
    readonly -A LT_DP_VERSION_SCALA=([2.0]="2.11" # On Dataproc images < 2.0, use Scala 2.11
                                     [2.1]="2.12" # On Dataproc images < 2.1, use Scala 2.12
                                     [2.2]="2.13" # On Dataproc images < 2.2, use Scala 2.13
                                    )
    # DATAPROC_IMAGE_VERSION is built from an environment variable set on the
    # cluster.  We will use this to determine the appropriate connector to use
    # based on the scala version.

    for LT_DP_VER in 2.0 2.1 2.2 ; do
      if ! test -v scala_version && compare_versions_lt $DATAPROC_IMAGE_VERSION ${LT_DP_VER} ; then
        local -r scala_version=${LT_DP_VERSION_SCALA[${LT_DP_VER}]}
        continue
      fi
    done

    if ! test -v scala_version ; then
      echo "unsupported DATAPROC_IMAGE_VERSION" >&2
      exit -1
    fi

    local -r jar_name="spark-bigquery-with-dependencies_${scala_version}-${version}.jar"

    echo "gs://spark-lib/bigquery/${jar_name}"
    return
  fi

  # hive-bigquery
  if [[ "${name}" == "hive-bigquery" ]]; then
    echo "gs://hadoop-lib/hive-bigquery/hive-bigquery-connector-${version}.jar"
    return
  fi

  # bigquery
  if [[ $(min_version "$DATAPROC_IMAGE_VERSION" 2.0) == 2.0 ]]; then
    local -r hadoop_version_suffix=hadoop3
  else
    local -r hadoop_version_suffix=hadoop2
  fi

  local -r jar_name="${name}-connector-${hadoop_version_suffix}-${version}.jar"

  echo "gs://hadoop-lib/${name}/${jar_name}"
}

validate_version() {
  local name=$1    # connector name: "bigquery" or "spark-bigquery"
  local version=$2 # connector version
  local min_valid_version=${MIN_CONNECTOR_VERSIONS[$name]}
  if [[ "$(min_version "$min_valid_version" "$version")" != "$min_valid_version" ]]; then
    echo "ERROR: ${name}-connector version should be greater than or equal to $min_valid_version, but was $version"
    return 1
  fi
}

update_connector_url() {
  local -r name=$1
  local -r url=$2

  if [[ -d ${VM_CONNECTORS_DATAPROC_DIR} ]]; then
    local vm_connectors_dir=${VM_CONNECTORS_DATAPROC_DIR}
  else
    local vm_connectors_dir=${VM_CONNECTORS_HADOOP_DIR}
  fi

  # Remove old connector if exists
  if [[ "${name}" == spark-bigquery || "${name}" == hive-bigquery ]]; then
    local pattern="${name}*.jar"
  else
    local pattern="${name}-connector-*.jar"
  fi

  find "${vm_connectors_dir}/" -name "$pattern" -delete

  gsutil cp -P "${url}" "${vm_connectors_dir}/"

  local -r jar_name=${url##*/}

  # Update or create version-less connector link
  ln -s -f "${vm_connectors_dir}/${jar_name}" "${vm_connectors_dir}/${name}-connector.jar"
}

update_connector_version() {
  local -r name=$1    # connector name: "bigquery" or "spark-bigquery"
  local -r version=$2 # connector version

  # validate new connector version
  validate_version "$name" "$version"

  local -r connector_url=$(get_connector_url "$name" "$version")

  update_connector_url "$name" "$connector_url"
}

update_connector() {
  local -r name=$1
  local -r version=$2
  local -r url=$3

  if [[ -n $version && -n $url ]]; then
    echo "ERROR: Both, connector version and URL are specified for the same connector"
    exit 1
  fi

  if [[ -n $version ]]; then
    update_connector_version "$name" "$version"
  fi

  if [[ -n $url ]]; then
    update_connector_url "$name" "$url"
  fi
}

if [[ -z $BIGQUERY_CONNECTOR_VERSION && -z $BIGQUERY_CONNECTOR_URL ]] &&
  [[ -z $HIVE_BIGQUERY_CONNECTOR_VERSION && -z $HIVE_BIGQUERY_CONNECTOR_URL ]] &&
  [[ -z $SPARK_BIGQUERY_CONNECTOR_VERSION && -z $SPARK_BIGQUERY_CONNECTOR_URL ]]; then
  echo "ERROR: None of connector versions or URLs are specified"
  exit 1
fi

update_connector "bigquery" "$BIGQUERY_CONNECTOR_VERSION" "$BIGQUERY_CONNECTOR_URL"
update_connector "spark-bigquery" "$SPARK_BIGQUERY_CONNECTOR_VERSION" "$SPARK_BIGQUERY_CONNECTOR_URL"
update_connector "hive-bigquery" "$HIVE_BIGQUERY_CONNECTOR_VERSION" "$HIVE_BIGQUERY_CONNECTOR_URL"
