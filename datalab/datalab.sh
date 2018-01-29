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

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
PROJECT=$(/usr/share/google/get_metadata_value ../project/project-id)
DOCKER_IMAGE=$(/usr/share/google/get_metadata_value attributes/docker-image || true)
SPARK_PACKAGES=$(/usr/share/google/get_metadata_value attributes/spark-packages || true)

if [[ "${ROLE}" == 'Master' ]]; then
  update_apt_get
  apt-get install -y -q docker.io
  if [[ -z "${DOCKER_IMAGE}" ]]; then
    DOCKER_IMAGE="gcr.io/cloud-datalab/datalab:local"
  fi
  gcloud docker -- pull ${DOCKER_IMAGE}

  # For some reason Spark has issues resolving the user's directory inside of
  # Datalab.
  # TODO(pmkc) consider fixing in Dataproc proper.
  SPARK_CONF='/etc/spark/conf/spark-defaults.conf'
  if ! grep -q '^spark\.sql\.warehouse\.dir=' "${SPARK_CONF}"; then
    echo 'spark.sql.warehouse.dir=/root/spark-warehouse' >> "${SPARK_CONF}"
  fi

  DATALAB_DIR="${HOME}/datalab"
  mkdir -p "${DATALAB_DIR}"

  # Expose every possible spark configuration to the container.
  VOLUMES=$(echo /etc/{hadoop*,hive*,*spark*} /usr/lib/hadoop/lib/{gcs,bigquery}*)
  VOLUME_FLAGS=$(echo ${VOLUMES} | sed 's/\S*/-v &:&/g')
  echo "VOLUME_FLAGS: ${VOLUME_FLAGS}"

  PYTHONPATH="/env/python:$(find /usr/lib/spark/python/lib -name '*.zip' | paste -sd:)"

  PYSPARK_SUBMIT_ARGS=''
  # Build PySpark Submit Arguments
  for package in $(echo ${SPARK_PACKAGES} | tr ',' ' '); do
    PYSPARK_SUBMIT_ARGS+="--packages ${package} "
  done
  PYSPARK_SUBMIT_ARGS+='pyspark-shell'

  # Java is too complicated to simply volume mount into the image, so we need
  # to install it in a child image.
  mkdir -p datalab-pyspark
  pushd datalab-pyspark
  cp -r /etc/apt/{trusted.gpg,sources.list.d} .
  cat << EOF > Dockerfile
FROM ${DOCKER_IMAGE}
ADD sources.list.d/backports.list /etc/apt/sources.list.d/
ADD sources.list.d/dataproc.list /etc/apt/sources.list.d/
ADD trusted.gpg /tmp/vm_trusted.gpg

RUN apt-key add /tmp/vm_trusted.gpg
RUN apt-get update
RUN apt-get install -y hive spark-python openjdk-8-jre-headless

ENV SPARK_HOME='/usr/lib/spark'
ENV JAVA_HOME='${JAVA_HOME}'
ENV PYTHONPATH='${PYTHONPATH}'
ENV PYTHONSTARTUP='/usr/lib/spark/python/pyspark/shell.py'
ENV PYSPARK_SUBMIT_ARGS='${PYSPARK_SUBMIT_ARGS}'
ENV DATALAB_ENV='GCE'
EOF
  docker build -t datalab-pyspark .
  popd

  # Docker gives a "too many symlinks" error if volumes are not yet automounted.
  # Ensure that the volumes are mounted to avoid the error.
  touch ${VOLUMES}
  if docker run -d --restart always --net=host -v "${DATALAB_DIR}:/content/datalab" ${VOLUME_FLAGS} \
      datalab-pyspark; then
    echo 'Cloud Datalab Jupyter server successfully deployed.'
    exit
  fi

  echo 'Failed to run Cloud Datalab' >&2
  exit 1
fi
