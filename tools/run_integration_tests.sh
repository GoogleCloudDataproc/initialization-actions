#!/bin/bash
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
#
# tools/run_integration_tests.sh (hadoop2 | hadoop3) <project_id> <service_account_email> <path_to_p12> [optional_maven_parameters, [...]]

set -Eeuo pipefail

HADOOP_VERSION=$1
export GCS_TEST_PROJECT_ID=$2
export GCS_TEST_SERVICE_ACCOUNT=$3
GCS_TEST_PRIVATE_KEYFILE=$4

print_usage() {
  echo -n "$0 (hadoop2 | hadoop3) <project ID> <service_account_email> <path_to_p12> [optional_maven_parameters, [...]]"
}

check_required_param() {
  local -r value=$1
  local -r error_msg=$2
  if [[ -z ${value} ]]; then
    echo "${error_msg}"
    print_usage
    exit 1
  fi
}

check_required_params() {
  check_required_param "${HADOOP_VERSION}" "Hadoop version required."
  check_required_param "${GCS_TEST_PROJECT_ID}" "Project ID required."
  check_required_param "${GCS_TEST_SERVICE_ACCOUNT}" "Service account email is required."
  check_required_param "${GCS_TEST_PRIVATE_KEYFILE}" "Private key file is required."

  if [[ ! -f "${GCS_TEST_PRIVATE_KEYFILE}" ]]; then
    echo "Can't find private key file ${GCS_TEST_PRIVATE_KEYFILE}"
    print_Usage
    exit 1
  fi
}

export HDFS_ROOT=file:///tmp

check_required_params

# When tests run, they run in the root of the module directory. Anything
# relative to our current directory won't work properly
GCS_TEST_PRIVATE_KEYFILE=$(readlink -f "${GCS_TEST_PRIVATE_KEYFILE}")
export GCS_TEST_PRIVATE_KEYFILE
export RUN_INTEGRATION_TESTS=true

./mvnw -B -e -T1C "-P${HADOOP_VERSION}" -Pintegration-test clean verify "${@:5}"
