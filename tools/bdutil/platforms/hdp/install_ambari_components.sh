# Copyright 2014 Google Inc. All Rights Reserved.
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

# Creates Ambari cluster based on Ambari's recommendations. Waits for the
# operations to finish.

loginfo "Setting up GCS connector cache cleaner and configuration."
if (( ${INSTALL_GCS_CONNECTOR} )) ; then
  if (( ${ENABLE_NFS_GCS_FILE_CACHE} )); then
    export GCS_METADATA_CACHE_TYPE='FILESYSTEM_BACKED'
    export GCS_FILE_CACHE_DIRECTORY="$(get_nfs_mount_point)"

    setup_cache_cleaner
  else
    export GCS_METADATA_CACHE_TYPE='IN_MEMORY'
    # For IN_MEMORY cache, this directory won't actually be used, but we set
    # it to a sane default for easy manual experimentation of file caching.
    export GCS_FILE_CACHE_DIRECTORY='/tmp/gcs_connector_metadata_cache'
  fi
fi

BLUEPRINT_NAME='recommended-blueprint'
BLUEPRINT_FILE="/tmp/${BLUEPRINT_NAME}.json"
CUSTOM_CONFIGURAITION_FILE='configuration.json'
CLUSTER_TEMPLATE_FILE='/tmp/cluster_template.json'
CONFIGURATION_RECOMMENDATION_FILE='/tmp/configuration_recommendation.json'
HOST_RECOMMENDATION_FILE='/tmp/host_recommendation.json'
RECOMMENDATION_ENDPOINT="${AMBARI_API}/stacks/${AMBARI_STACK}/\
versions/${AMBARI_STACK_VERSION}/recommendations"
JSON_SERVICES_ARRAY="[ \"$(sed 's/ /\",\"/g' <<< ${AMBARI_SERVICES})\" ]"
JSON_HOST_ARRAY="[ \"$(hostname --fqdn)\",
    $(xargs -n 1 host -Tta <<< ${WORKERS[@]} \
        | awk '{print "\""$1"\""}' \
        | paste -sd,)]"

# Make variable substitutions in configurations blueprint.
loginfo "Replacing variables in ${CUSTOM_CONFIGURAITION_FILE}."
perl -pi -e 's/\$\{([^\}]*)\}/$ENV{$1}/e' ${CUSTOM_CONFIGURAITION_FILE}

# Wait for all hosts to register:
loginfo "Waiting for all hosts to register."
ambari_wait "${AMBARI_CURL} ${AMBARI_API}/hosts | grep -c host_name" \
    $(( ${NUM_WORKERS} + 1 ))

# Get recommendations
loginfo "Getting configuration recommendation from ambari-server."
${AMBARI_CURL} -X POST ${RECOMMENDATION_ENDPOINT} \
    -d "{
          \"recommend\" : \"configurations\",
          \"services\" : ${JSON_SERVICES_ARRAY},
          \"hosts\" : ${JSON_HOST_ARRAY}
        }" \
    > ${CONFIGURATION_RECOMMENDATION_FILE}

loginfo "Getting host group recommendation from ambari-server."
${AMBARI_CURL} -X POST ${RECOMMENDATION_ENDPOINT} \
    -d "{
          \"recommend\" : \"host_groups\",
          \"services\" : ${JSON_SERVICES_ARRAY},
          \"hosts\" : ${JSON_HOST_ARRAY}
        }" \
    > ${HOST_RECOMMENDATION_FILE}

# Fix Python 2.6 on CentOS
if ! python -c 'import argparse' && [[ -x $(which yum) ]]; then
  yum install -y python-argparse
fi

loginfo "Converting recommendations to blueprint."
./create_blueprint.py \
    --conf_recommendation ${CONFIGURATION_RECOMMENDATION_FILE} \
    --host_recommendation ${HOST_RECOMMENDATION_FILE} \
    --blueprint ${BLUEPRINT_FILE} \
    --cluster_template ${CLUSTER_TEMPLATE_FILE} \
    --blueprint_name ${BLUEPRINT_NAME} \
    --custom_configuraton ${CUSTOM_CONFIGURAITION_FILE}

loginfo "Posting ${BLUEPRINT_FILE} to ambari-server."
${AMBARI_CURL} -X POST -d @${BLUEPRINT_FILE} \
    ${AMBARI_API}/blueprints/${BLUEPRINT_NAME}

# very hacky functions to get to the json, should convert to python.
# too bad 'jq' isn't in the standard CentOS packages
loginfo "Provisioning ambari cluster."
${AMBARI_CURL} -X POST -d @${CLUSTER_TEMPLATE_FILE} \
    ${AMBARI_API}/clusters/${PREFIX}

# Poll for completion
loginfo "Waiting for ambari cluster creation to complete (may take awhile)."
ambari_wait_requests_completed

# Set up HDFS /user directories.
loginfo "Setting up HDFS /user directories."
for USER in $(getent passwd | grep '/home' | cut -d ':' -f 1); do
  echo "Creating HDFS directory for user '${USER}'"
  sudo -u hdfs hdfs dfs -mkdir -p "/user/${USER}"
  sudo -u hdfs hdfs dfs -chown "${USER}" "/user/${USER}"
done
