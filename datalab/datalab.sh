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

set -exo pipefail

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly PROJECT="$(/usr/share/google/get_metadata_value ../project/project-id)"
readonly SPARK_PACKAGES="$(/usr/share/google/get_metadata_value attributes/spark-packages || true)"
readonly SPARK_CONF='/etc/spark/conf/spark-defaults.conf'
readonly DATALAB_DIR="${HOME}/datalab"
readonly PYTHONPATH="/env/python:$(find /usr/lib/spark/python/lib -name '*.zip' | paste -sd:)"
readonly DOCKER_IMAGE="$(/usr/share/google/get_metadata_value attributes/docker-image || \
  echo 'gcr.io/cloud-datalab/datalab:local')"

# Expose every possible spark configuration to the container.
readonly VOLUMES="$(echo /etc/{hadoop*,hive*,*spark*} /usr/lib/hadoop/lib/{gcs,bigquery}*)"
readonly VOLUME_FLAGS="$(echo "${VOLUMES}" | sed 's/\S*/-v &:&/g')"

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function docker_pull() {
  for ((i = 0; i < 10; i++)); do
    if (gcloud docker -- pull $1); then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function configure_master(){
  update_apt_get || err 'Failed to update apt-get'
  mkdir -p "${DATALAB_DIR}"

  apt-get install -y -q docker.io || err 'Failed to install Docker'

  docker_pull ${DOCKER_IMAGE} || err "Failed to pull ${DOCKER_IMAGE}"

  # For some reason Spark has issues resolving the user's directory inside of
  # Datalab.
  # TODO(pmkc) consider fixing in Dataproc proper.
  if ! grep -q '^spark\.sql\.warehouse\.dir=' "${SPARK_CONF}"; then
    echo 'spark.sql.warehouse.dir=/root/spark-warehouse' >> "${SPARK_CONF}"
  fi

  # Docker gives a "too many symlinks" error if volumes are not yet automounted.
  # Ensure that the volumes are mounted to avoid the error.
  touch ${VOLUMES}

  # Build PySpark Submit Arguments
  pyspark_submit_args=''
  for package in ${SPARK_PACKAGES//','/' '}; do
    pyspark_submit_args+="--packages ${package} "
  done
  pyspark_submit_args+='pyspark-shell'

  # Java is too complicated to simply volume mount into the image, so we need
  # to install it in a child image.
  mkdir -p datalab-pyspark
  pushd datalab-pyspark
  cp /etc/apt/trusted.gpg .
  cp /etc/apt/sources.list.d/backports.list .
  cp /etc/apt/sources.list.d/dataproc.list .
  cat << EOF > Dockerfile
FROM ${DOCKER_IMAGE}
ADD backports.list /etc/apt/sources.list.d/
ADD dataproc.list /etc/apt/sources.list.d/
ADD trusted.gpg /tmp/vm_trusted.gpg

RUN apt-key add /tmp/vm_trusted.gpg
RUN apt-get update
RUN apt-get install -y hive spark-python openjdk-8-jre-headless

ENV SPARK_HOME='/usr/lib/spark'
ENV JAVA_HOME='${JAVA_HOME}'
ENV PYTHONPATH='${PYTHONPATH}'
ENV PYTHONSTARTUP='/usr/lib/spark/python/pyspark/shell.py'
ENV PYSPARK_SUBMIT_ARGS='${pyspark_submit_args}'
ENV DATALAB_ENV='GCE'
EOF
  docker build -t datalab-pyspark .
  popd

}

function run_datalab(){
  if docker run -d --restart always --net=host \
      -v "${DATALAB_DIR}:/content/datalab" ${VOLUME_FLAGS} datalab-pyspark; then
    echo 'Cloud Datalab Jupyter server successfully deployed.'
  else
    err 'Failed to run Cloud Datalab'
  fi
}

function main(){
  if [[ "${ROLE}" == 'Master' ]]; then
    configure_master
    run_datalab
  fi
}

main
