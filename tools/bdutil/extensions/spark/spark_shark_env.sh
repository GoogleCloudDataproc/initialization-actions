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

# This file contains environment-variable overrides to be used in conjunction
# with bdutil_env.sh in order to deploy a Hadoop + Spark + Shark cluster.
# Usage: ./bdutil deploy -e extensions/spark/spark_shark_env.sh

import_env extensions/spark/spark_env.sh

# URIs of tarballs to install.
SHARK_HADOOP1_TARBALL_URI='gs://spark-dist/shark-0.9.1-bin-hadoop1.tgz'
SHARK_HADOOP2_TARBALL_URI='gs://spark-dist/shark-0.9.1-bin-hadoop2.tgz'
# Shark is not compatible with Spark 1.x
SPARK_HADOOP1_TARBALL_URI='gs://spark-dist/spark-0.9.2-bin-hadoop1.tgz'
SPARK_HADOOP2_TARBALL_URI='gs://spark-dist/spark-0.9.2-bin-hadoop2.tgz'

# Directory on each VM in which to install shark
SHARK_INSTALL_DIR='/home/hadoop/shark-install'

# Value to give Shark indicating the amount of Spark worker memory
# available/usable by Shark per worker. Expressed as a fraction of total
# physical memory.
SHARK_MEM_FRACTION='0.8'

COMMAND_GROUPS+=(
  "install_shark:
     extensions/spark/install_shark.sh
  "
)

# Installation of shark
COMMAND_STEPS+=(
  'install_shark,install_shark'
)
