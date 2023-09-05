#!/bin/bash
#
# Copyright 2023 Google LLC and contributors
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
#
# Initialization action for executing initialization actions in serial where
# order cannot otherwise be specified
#
# For more information in init actions and Google Cloud Dataproc see the Cloud
# Dataproc documentation at https://cloud.google.com/dataproc/init-actions

set -euxo pipefail

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
readonly OS_NAME

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}
REGION=us-west1
INIT_ACTIONS_ROOT_DEFAULT="gs://goog-dataproc-initialization-actions-${REGION}"
INIT_ACTIONS_ROOT=$(get_metadata_attribute INIT_ACTIONS_ROOT ${INIT_ACTIONS_ROOT_DEFAULT})

initialization_action_paths=$(get_metadata_attribute "initialization-action-paths" "")
initialization_action_paths_separator=$(get_metadata_attribute "initialization-action-paths-separator" ";")

if [[ -n "${initialization_action_paths}" ]]; then
  INIT_ACTION_LIST=($(echo $initialization_action_paths | sed -e 's/;/ /'))
  for uri in ${INIT_ACTION_LIST[@]}; do
    BASENAME="$(basename $uri)"
    echo "executing script: $uri"
    gsutil cp ${uri} ${BASENAME}
    chmod u+x ${BASENAME}
    echo -n "launching initialization action ${BASENAME}..."
    #./${BASENAME}
    echo "Done"
  done
else
  echo "no initialization-action-paths provided.  Nothing to do."
  exit 1
fi
