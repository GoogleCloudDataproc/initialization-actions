#!/bin/bash
# Copyright 2016 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This init script installs Google Cloud Datalab on the master node of a
# Dataproc cluster.

set -e -x

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
PROJECT=$(/usr/share/google/get_metadata_value ../project/project-id)
DOCKER_IMAGE=$(/usr/share/google/get_metadata_value attributes/docker-image || true)

if [[ "${ROLE}" == 'Master' ]]; then
  apt-get update
  apt-get install -y -q docker.io
  if [[ -z "${DOCKER_IMAGE}" ]]; then
    DOCKER_IMAGE="gcr.io/cloud-datalab/datalab:local"
  fi
  gcloud docker pull ${DOCKER_IMAGE}

  # For some reason Spark has issues resolving the user's directory inside of
  # Datalab.
  # TODO(pmkc) consider fixing in Dataproc proper.
  SPARK_CONF='/etc/spark/conf/spark-defaults.conf'
  if ! grep -q '^spark\.sql\.warehouse\.dir=' "${SPARK_CONF}"; then
    echo 'spark.sql.warehouse.dir=/root/spark-warehouse' >> "${SPARK_CONF}"
  fi

  DATALAB_DIR="${HOME}/datalab"
  mkdir -p "${DATALAB_DIR}"

  # Expose every possbile spark libary to the container.
  VOLUME_FLAGS=$(echo {/usr/bin,/usr/lib,/etc,/etc/alternatives,/var/log}/{hadoop*,hive*,*spark*} \
      /hadoop* /usr/lib/jvm/ /usr/share/java/ /usr/lib/bigtop* \
      | sed 's/\S*/-v &:&/g')
  echo "VOLUME_FLAGS: ${VOLUME_FLAGS}"

  PYTHONPATH="/env/python:$(find /usr/lib/spark/python/lib -name '*.zip' | paste -sd:)"
  # Using this many volumes, each containing symlinks, often gives a "too many
  # symlinks" error. Just retry till it works.
  for i in {1..10}; do
    if docker run -d --net=host -v "${DATALAB_DIR}:/content/datalab" ${VOLUME_FLAGS} \
        -e "SPARK_HOME=/usr/lib/spark" \
        -e "JAVA_HOME=${JAVA_HOME}" \
        -e "PYTHONPATH=${PYTHONPATH}" \
        -e "PYTHONSTARTUP=/usr/lib/spark/python/pyspark/shell.py" \
        -e "DATALAB_ENV=GCE" \
        ${DOCKER_IMAGE}; then
      echo 'Cloud Datalab Jupyter server successfully deployed.'
      exit
    fi
    sleep 1
  done

  echo 'Failed to run Cloud Datalab' >&2
  exit 1
fi
