#!/usr/bin/env bash

# Copyright 2014 Google Inc. All Rights Reserved.

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


## install_gcs_connector_on_ambari.sh
## This file:
##   * downloads the relevant gcs-connector-<version>.jar
##   * installs into a local lib dir
##   * adds that lib dir to relevant classpaths

if (( ${INSTALL_GCS_CONNECTOR} )) ; then
loginfo "installing GCS_CONNECTOR_JAR on each node"
  LIB_JARS_DIR="${HADOOP_INSTALL_DIR}/lib"
  mkdir -p ${LIB_JARS_DIR}

  # Grab the connector jarfile, add it to installation /lib directory.
  JARNAME=$(grep -o '[^/]*\.jar' <<< ${GCS_CONNECTOR_JAR})
  LOCAL_JAR="${LIB_JARS_DIR}/${JARNAME}"

  download_bd_resource "${GCS_CONNECTOR_JAR}" "${LOCAL_JAR}"

  # link gcs connector into main hadoop lib dir
  source <(grep "^export HADOOP_HOME=" /etc/hadoop/conf.empty/hadoop-env.sh) || true
  ln -sv "${LOCAL_JAR}" "${HADOOP_HOME}/lib/"
fi
