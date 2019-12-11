#!/usr/bin/env bash

function_exists() {
  declare -f -F $1 >/dev/null
  return $?
}

throw() {
  echo "$*" >&2
  echo
  function_exists usage && usage
  exit 1
}

get_metadata_property() {
  [[ -z $1 ]] && throw "missing function param for DATAPROC_CLUSTER_NAME" || DATAPROC_CLUSTER_NAME=$1
  [[ -z $2 ]] && throw "missing function param for METADATA_KEY" || METADATA_KEY=$2
  # Get $DATAPROC_CLUSTER_NAME metadata value for key $METADATA_KEY...
  gcloud dataproc clusters describe $DATAPROC_CLUSTER_NAME | python -c "import sys,yaml; cluster = yaml.load(sys.stdin); print(cluster['config']['gceClusterConfig']['metadata']['$METADATA_KEY'])"
}
