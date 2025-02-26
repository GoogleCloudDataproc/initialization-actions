#!/bin/bash
#
# Copyright 2025 Google LLC and contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#   This initialization script installs the required
#   jars and sets the hive conf to enable lineage.

set -euxo pipefail

function prepare_env() {
  export HIVE_HOME="/usr/lib/hive"
  export HIVE_CONF_DIR="/etc/hive/conf"
  export HIVE_CONF_FILE="$HIVE_CONF_DIR/hive-site.xml"
  export HIVE_LIB_DIR="$HIVE_HOME/lib"
  export INSTALLATION_SOURCE="gs://hadoop-lib/hive-lineage"
  export HIVE_OL_HOOK_VERSION="1.0.0-preview"
  export HIVE_OL_HOOK="io.openlineage.hive.hooks.HiveOpenLineageHook"
}

function set_hive_lineage_conf() {
  declare -A properties=(
    ["hive.exec.post.hooks"]="$HIVE_OL_HOOK"
    ["hive.exec.failure.hooks"]="$HIVE_OL_HOOK"
    ["hive.openlineage.transport.type"]="gcplineage"
    ["hive.security.authorization.sqlstd.confwhitelist.append"]="tez.application.tags|hive.openlineage.*"
    ["hive.conf.validation"]="false"
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
  echo "Installing openlineage-hive hook"
  gsutil cp -P "$INSTALLATION_SOURCE/hive-openlineage-hook-$HIVE_OL_HOOK_VERSION.jar" "$HIVE_LIB_DIR/hive-openlineage-hook.jar"
}

function restart_hive_server2_master() {
  ROLE=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/dataproc-role)
  if [[ "${ROLE}" == 'Master' ]]; then
    echo "Restarting hive-server2"
    sudo systemctl restart hive-server2.service
  fi
}

prepare_env
install_jars
set_hive_lineage_conf
restart_hive_server2_master
