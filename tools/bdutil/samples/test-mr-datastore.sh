#!/usr/bin/env bash
#
# Copyright 2013 Google Inc. All Rights Reserved.
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

###############################################################################
# Sets up and runs datastore for WordCount job to verify CloudDatastore setup.
# Note this clears and repopulates Kinds within the given dataset.
# Usage: ./bdutil -v -u "samples/*" run_command ./test-mr-datastore.sh
################################################################################

set -e

source hadoop-env-setup.sh

# Check for existence of jars
for JAR in datastore_wordcountsetup.jar datastore_wordcount.jar; do
  if ! [[ -r ${JAR} ]]; then
    echo "Error. Could not find jar: ${JAR}" >&2
    exit 1
  fi
done

# Upload README.txt
hadoop jar datastore_wordcountsetup.jar ${PROJECT} hadoopSampleWordCountLine \
    hadoopSampleWordCountCount ${HADOOP_INSTALL_DIR}/README.txt

#  Perform word count MapReduce on README.txt
hadoop jar datastore_wordcount.jar ${PROJECT} hadoopSampleWordCountLine \
    hadoopSampleWordCountCount wordcount

echo 'Word count finished successfully.'
