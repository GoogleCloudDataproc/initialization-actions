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

# tools/run_integration_tests [project_id] [clinet id] [client secret] [service_account_email] [path_to_p12]

set -o errexit
set -o nounset

export GCS_TEST_PROJECT_ID=$1
export GCS_TEST_CLIENT_ID=$2
export GCS_TEST_CLIENT_SECRET=$3
export GCS_TEST_SERVICE_ACCOUNT=$4
export GCS_TEST_PRIVATE_KEYFILE=$5

function print_usage() {
  echo -n "$0 [project ID] [clinet ID] [client secret] [service_account_email] [path_to_p12]"
}

function check_required_params () {
  if [ "x" = "x$GCS_TEST_PROJECT_ID" ]; then
    echo "Project ID required".
    print_usage
    exit 1
  fi

  if [ "x" = "x$GCS_TEST_CLIENT_ID" ]; then
    echo "Client ID is required"
    print_usage
    exit 1
  fi

  if [ "x" = "x$GCS_TEST_CLIENT_SECRET" ]; then
    echo "Client secret is required."
    print_usage
    exit 1
  fi

  if [ "x" = "x$GCS_TEST_SERVICE_ACCOUNT" ]; then
    echo "Service account email is required."
    print_usage
    exit 1
  fi

  if [ "x" = "x$GCS_TEST_PRIVATE_KEYFILE" ]; then
    echo "Private key file is required."
    print_usage
    exit 1
   fi

  if [ ! -f "$GCS_TEST_PRIVATE_KEYFILE" ]; then
    echo "Can't find private key file $GCS_TEST_PRIVATE_KEYFILE"
    print_Usage
    exit 1
  fi
}

export HDFS_ROOT=file:///tmp

check_required_params

# When tests run, they run in the root of the module directory. Anything
# relative to our current directory won't work properly
export GCS_TEST_PRIVATE_KEYFILE=$(readlink -f "$GCS_TEST_PRIVATE_KEYFILE")
export RUN_INTEGRATION_TESTS=true

if ! which mvn; then
  echo "Couldn't find mvn on the PATH"
  exit 1
fi

mvn integration-test
