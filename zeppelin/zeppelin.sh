#!/bin/bash
# Copyright 2015 Google, Inc.
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
# limitations under the License.set -x -e

# This init script installs Apache Zeppelin on the master node of a Cloud
# Dataproc cluster. Zeppelin is also configured based on the size of your
# cluster and the versions of Spark/Hadoop which are installed.
set -x -e

# Only run on the master node
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
if [[ "${ROLE}" == 'Master' ]]; then
  # Install zeppelin
  apt-get install -y zeppelin
fi
