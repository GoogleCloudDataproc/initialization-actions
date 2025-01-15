#!/bin/bash

#   This initialization script installs the required
#   jars and sets the hive conf to enable lineage.
#
#   To use this script, add the following using the
#   --initialization-actions flag during cluster creation.
#   gs://dataproc-hive-lineage-prototype/v3/initialization-actions/enable_lineage.sh

set -euxo pipefail

export HIVE_HOME="/usr/lib/hive"
export HIVE_CONF_DIR="/etc/hive/conf"
export HIVE_CONF_FILE="$HIVE_CONF_DIR/hive-site.xml"
export HIVE_LIB_DIR="/usr/lib/hive/lib"
export INSTALLATION_SOURCE="gs://dataproc-hive-lineage-prototype/v3/jars" # TODO: Update the gcs bucket once finalised
export HIVE_OL_HOOK="io.openlineage.hive.hooks.HiveOpenLineageHook"


function set_hive_lineage_conf() {
  declare -A properties=(
    ["hive.exec.post.hooks"]="$HIVE_OL_HOOK"
    ["hive.exec.exec.hooks"]="$HIVE_OL_HOOK"
    ["hive.openlineage.transport.type"]="gcplineage"
    ["hive.conf.validation"]="false" # to allow custom properties, like hive.openlineage.namespace
  )
  echo "Setting hive conf to enable lineage"
  for key in "${!properties[@]}"; do
    bdconfig set_property \
      --configuration_file="$HIVE_CONF_FILE" \
      --name "$key" \
      --value "${properties[$key]}"
  done
}

function install_jars() {
  # TODO: Allow customisation of the jar version
  echo "Installing openlineage-hive hook"
  gsutil cp -P "$INSTALLATION_SOURCE/hive-openlineage-hook-shaded.jar" "$HIVE_LIB_DIR/hive-openlineage-hook.jar"
}

function restart_hive_server2_master() {
  ROLE=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/dataproc-role)
  if [[ "${ROLE}" == 'Master' ]]; then
    echo "Restarting hive-server2"
    sudo systemctl restart hive-server2.service
  fi
}

install_jars
set_hive_lineage_conf
restart_hive_server2_master
