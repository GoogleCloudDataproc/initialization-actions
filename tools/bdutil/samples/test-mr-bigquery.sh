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
# Sets up and runs WordCount job to verify BigQuery setup.
# Usage: ./bdutil -v -u "samples/*" run_command ./test-mr-bigquery.sh [outputTable]
################################################################################

set -e

source hadoop-env-setup.sh

OUTPUT_TABLE=$1

if [[ -z "${OUTPUT_TABLE}" ]]; then
  echo "Error. test-mr-biquery.sh requires one argument the fully quallified" \
      "BigQuery table ID to output to." >&2
  exit 1
fi

INPUT_TABLE='publicdata:samples.shakespeare'
INPUT_TABLE_FIELD='word'
JAR='bigquery_wordcount.jar'

# Check for existence of jar
if ! [[ -r ${JAR} ]]; then
  echo "Error. Could not find jar: ${JAR}" >&2
  exit 1
fi

#  Perform word count MapReduce on README.txt
hadoop jar ${JAR} ${PROJECT} ${INPUT_TABLE} ${INPUT_TABLE_FIELD} ${OUTPUT_TABLE}

echo 'Word count finished successfully.'
