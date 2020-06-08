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

readonly NOT_SUPPORTED_MESSAGE="Datalab initialization action is not supported on Dataproc 2.0+.
Use Jupyter Component instead: https://cloud.google.com/dataproc/docs/concepts/components/jupyter"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly PROJECT="$(/usr/share/google/get_metadata_value ../project/project-id)"
readonly SPARK_PACKAGES="$(/usr/share/google/get_metadata_value attributes/spark-packages || true)"
readonly SPARK_CONF='/etc/spark/conf/spark-defaults.conf'
readonly DATALAB_DIR="${HOME}/datalab"
readonly PYTHONPATH="/env/python:$(find /usr/lib/spark/python/lib -name '*.zip' | paste -sd:)"
readonly DOCKER_IMAGE="$(/usr/share/google/get_metadata_value attributes/docker-image ||
  echo 'gcr.io/cloud-datalab/datalab:local')"

# For running the docker init action
readonly DEAFULT_INIT_ACTIONS_REPO=gs://dataproc-initialization-actions
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_REPO ||
  echo ${DEAFULT_INIT_ACTIONS_REPO})"
readonly INIT_ACTIONS_BRANCH="$(/usr/share/google/get_metadata_value attributes/INIT_ACTIONS_BRANCH ||
  echo 'master')"

# Expose every possible spark configuration to the container.
VOLUMES="$(echo /etc/{hadoop*,hive*,*spark*})"

CONNECTORS_LIB=/usr/lib/hadoop/lib
if [[ -d /usr/local/share/google/dataproc/lib ]]; then
  CONNECTORS_LIB="/usr/local/share/google/dataproc/lib"
fi
if [[ -L ${CONNECTORS_LIB}/gcs-connector.jar ]]; then
  VOLUMES+=" ${CONNECTORS_LIB}/gcs-connector.jar"
else
  VOLUMES+=" $(compgen -G ${CONNECTORS_LIB}/gcs*)"
fi
if [[ -L ${CONNECTORS_LIB}/bigquery-connector.jar ]]; then
  VOLUMES+=" ${CONNECTORS_LIB}/bigquery-connector.jar"
elif compgen -G "${CONNECTORS_LIB}/bigquery*" >/dev/null; then
  VOLUMES+=" $(compgen -G ${CONNECTORS_LIB}/bigquery*)"
fi

readonly VOLUMES
readonly VOLUME_FLAGS="$(echo "${VOLUMES}" | sed 's/\S*/-v &:&/g')"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  return 1
}

function install_docker() {
  # Run the docker init action to install docker.
  local init_actions_dir
  init_actions_dir=$(mktemp -d -t dataproc-init-actions-XXXX)
  if [[ ${INIT_ACTIONS_REPO} == gs://* ]]; then
    gsutil -m rsync -r "${INIT_ACTIONS_REPO}" "${init_actions_dir}"
  else
    git clone -b "${INIT_ACTIONS_BRANCH}" --single-branch "${INIT_ACTIONS_REPO}" "${init_actions_dir}"
  fi
  find "${init_actions_dir}" -name '*.sh' -exec chmod +x {} \;
  "${init_actions_dir}/docker/docker.sh"
}

function docker_pull() {
  for ((i = 0; i < 10; i++)); do
    if (gcloud docker -- pull "$1"); then
      return 0
    fi
    sleep 5
  done
  return 1
}

function configure_master() {
  mkdir -p "${DATALAB_DIR}"

  docker_pull "${DOCKER_IMAGE}" || err "Failed to pull ${DOCKER_IMAGE}"

  # For some reason Spark has issues resolving the user's directory inside of
  # Datalab.
  # TODO(pmkc) consider fixing in Dataproc proper.
  if ! grep -q '^spark\.sql\.warehouse\.dir=' "${SPARK_CONF}"; then
    echo 'spark.sql.warehouse.dir=/root/spark-warehouse' >>"${SPARK_CONF}"
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
  cp /etc/apt/sources.list.d/dataproc.list .
  cat <<EOF >Dockerfile
FROM ${DOCKER_IMAGE}

# Enabling APT to download from HTTPS repository.
RUN apt-get update
RUN apt-get install -y apt-transport-https software-properties-common

ADD dataproc.list /etc/apt/sources.list.d/
ADD trusted.gpg /tmp/vm_trusted.gpg
RUN apt-key add /tmp/vm_trusted.gpg

# Add Ubuntu 18.04 LTS (bionic) repository to Ubuntu 16.04 LTS (xenial) container,
# so pacakges built on Debian 10 can pull in their dependencies.
RUN add-apt-repository 'deb http://archive.ubuntu.com/ubuntu bionic main'

RUN apt-get update
RUN apt-get install -y hive spark-python openjdk-8-jre-headless

# Workers do not run docker, so have a different python environment.
# To run python3, you need to run the conda init action.
# The conda init action correctly sets up python in PATH and
# /etc/spark/conf/spark-env.sh, but running pyspark via shell.py does
# not pick up spark-env.sh. So, set PYSPARK_PYTHON explicitly to either
# system python or conda python. It is on the user to set up the same
# version of python for workers and the datalab docker container.
ENV PYSPARK_PYTHON=$(ls /opt/conda/bin/python || command -v python)

ENV SPARK_HOME='/usr/lib/spark'
ENV JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
ENV PYTHONPATH='${PYTHONPATH}'
ENV PYTHONSTARTUP='/usr/lib/spark/python/pyspark/shell.py'
ENV PYSPARK_SUBMIT_ARGS='${pyspark_submit_args}'
ENV DATALAB_ENV='GCE'
EOF
  docker build -t datalab-pyspark .
  popd
}

function run_datalab() {
  if docker run -d --restart always --net=host \
    -v "${DATALAB_DIR}:/content/datalab" ${VOLUME_FLAGS} datalab-pyspark; then
    echo 'Cloud Datalab Jupyter server successfully deployed.'
  else
    err 'Failed to run Cloud Datalab'
  fi
}

function main() {
  if [[ "${ROLE}" == 'Master' ]]; then
    install_docker
    configure_master
    run_datalab
  fi
}

main
