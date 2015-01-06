#!/bin/bash
#
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
#
###############################################################################
#
# Run word count using hadoop-streaming and external scripts.
# This script assumes the mapper and reducer scripts are in this directory.
#
# Run this script without any command line options to run a word count
# over the Shakespeare corpus. Note that we are counting the number of
# times a word appears in the shakespeare index, which is not the same
# as the number of times that word appears in Shakespeare's works.
#
# Here is an alternate command to count the number of times a github
# repository is mentioned in the github_timeline table:
# $ streaming_word_count.sh \
#     --input_table github_timeline --input_field repository_name
#
# Add the option --stream_output to enable streaming output back to BigQuery
# (input is always streamed from BigQuery when running this script).

SCRIPT_DIR=$(dirname $0)
CLASS_PREFIX='com.google.cloud.hadoop.io.bigquery.mapred.'

STREAMING_JAR="${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar"
LIBJARS=''
WORD_SCHEMA="{'name': 'Word','type': 'STRING'}"
NUMBER_SCHEMA="{'name': 'Count','type': 'INTEGER'}"

INPUT_PROJECT='publicdata'
INPUT_DATASET='samples'
INPUT_TABLE='shakespeare'
INPUT_FIELD_NAME='word'
INPUT_FORMAT="${CLASS_PREFIX}BigQueryMapredInputFormat"
INPUT_FILE='placeholder'

STREAM_OUTPUT=false
OUTPUT_PROJECT='myProject'
OUTPUT_DATASET='testdataset'
OUTPUT_TABLE='testtable'
OUTPUT_SCHEMA="[${WORD_SCHEMA},${NUMBER_SCHEMA}]"
OUTPUT_FORMAT="${CLASS_PREFIX}BigQueryMapredOutputFormat"
OUTPUT_COMMITTER_CLASS="${CLASS_PREFIX}BigQueryMapredOutputCommitter"
OUTPUT_DIR_NOT_STREAMING='output/test'
OUTPUT_DIR_STREAMING='placeholder'

MAPPER_NAME='word_count_mapper.py'
MAPPER_FILE="${SCRIPT_DIR}/${MAPPER_NAME}"
MAPPER_COMMAND="python ${MAPPER_NAME} ${INPUT_FIELD_NAME}"
REDUCER_NAME='word_count_reducer.py'
REDUCER_FILE="${SCRIPT_DIR}/${REDUCER_NAME}"
REDUCER_COMMAND_NOT_STREAMING="python ${REDUCER_NAME}"
REDUCER_COMMAND_STREAMING="python ${REDUCER_NAME} --output_json"

function error_exit() {
  echo "$@" 1>&2
  exit 1
}

function echo_and_run() {
  echo "$@"
  "$@"
}

function parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      "--input_dataset")
        INPUT_DATASET="$2"
        shift
        shift
        ;;
      "--input_field")
        INPUT_FIELD_NAME="$2"
        shift
        shift
        ;;
      "--input_project")
        INPUT_PROJECT="$2"
        shift
        shift
        ;;
      "--input_table")
        INPUT_TABLE="$2"
        shift
        shift
        ;;
      "--libjars")
        LIBJARS="${LIBJARS}${LIBJARS:+,}$2"
        shift
        shift
        ;;
      "--output_dataset")
        OUTPUT_DATASET="$2"
        shift
        shift
        ;;
      "--output_project")
        OUTPUT_PROJECT="$2"
        shift
        shift
        ;;
      "--output_schema")
        OUTPUT_SCHEMA="$2"
        shift
        shift
        ;;
      "--output_table")
        OUTPUT_TABLE="$2"
        shift
        shift
        ;;
      "--stream_output")
        STREAM_OUTPUT=true
        shift
        ;;
      *)
        error_exit "Unknown command line argument '$1'"
        ;;
    esac
  done
}

function hadoop_streaming() {
  if [[ ${STREAM_OUTPUT} == true ]]; then
    echo 'Streaming both input and output.'
    echo_and_run hadoop jar ${STREAMING_JAR} \
      ${LIBJARS:+-libjars} ${LIBJARS} \
      -files ${MAPPER_FILE},${REDUCER_FILE} \
      -D mapred.bq.input.project.id="${INPUT_PROJECT}" \
      -D mapred.bq.input.dataset.id="${INPUT_DATASET}" \
      -D mapred.bq.input.table.id="${INPUT_TABLE}" \
      -D mapred.bq.output.project.id="${OUTPUT_PROJECT}" \
      -D mapred.bq.output.dataset.id="${OUTPUT_DATASET}" \
      -D mapred.bq.output.table.id="${OUTPUT_TABLE}" \
      -D mapred.bq.output.table.schema="${OUTPUT_SCHEMA}" \
      -D mapred.output.committer.class="${OUTPUT_COMMITTER_CLASS}" \
      -inputformat ${INPUT_FORMAT} \
      -outputformat ${OUTPUT_FORMAT} \
      -input "${INPUT_FILE}" \
      -output "${OUTPUT_DIR_STREAMING}" \
      -mapper "${MAPPER_COMMAND}" \
      -reducer "${REDUCER_COMMAND_STREAMING}"
  else
    echo 'Streaming input only.'
    echo_and_run hadoop jar ${STREAMING_JAR} \
      ${LIBJARS:+-libjars} ${LIBJARS} \
      -files ${MAPPER_FILE},${REDUCER_FILE} \
      -D mapred.bq.input.project.id=${INPUT_PROJECT} \
      -D mapred.bq.input.dataset.id=${INPUT_DATASET} \
      -D mapred.bq.input.table.id=${INPUT_TABLE} \
      -inputformat ${INPUT_FORMAT} \
      -input ${INPUT_FILE} \
      -output ${OUTPUT_DIR_NOT_STREAMING} \
      -mapper "${MAPPER_COMMAND}" \
      -reducer "${REDUCER_COMMAND_NOT_STREAMING}"
  fi
}

# Remove the BigQuery intermediate files created by streaming-input.
# This function assumes that neither the BiQuery export file location
# (mapred.bq.temp.gcs.path) nor the bucket (mapred.bq.gcs.bucket)
# have been overridden when calling hadoop-streamimg.
function cleanup() {
  local conf_file="${HADOOP_HOME}/conf/core-site.xml"
  local bucket=$(grep -A 1 fs.gs.system.bucket "${conf_file}" \
    | tail -1 \
    | sed -e 's/^[[:space:]]*//' -e 's/<[^>]*>//g')
  local tmp_dir_pattern='hadoop/tmp/bigquery/job_????????????_????'
  if [[ -n "${bucket}" ]]; then
    echo hadoop fs -rmr "gs://${bucket}/${tmp_dir_pattern}"
    hadoop fs -rmr "gs://${bucket}/${tmp_dir_pattern}"
  else
    echo "Could not find fs.gs.system.bucket in Hadoop conf file ${conf_file}"
  fi
}

function main() {
  parse_args "$@"
  hadoop_streaming
  cleanup
}

main "$@"
