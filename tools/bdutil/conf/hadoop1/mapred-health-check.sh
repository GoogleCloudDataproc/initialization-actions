#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Check to see if the TaskTracker is healthy by checking it's http address.
# Necessary to avoid [MAPREDUCE-4668].

# Redirect stderr to stdout.
# Necessary to see problems with health check script in log.
# Will only show stdout if ERROR is present at the beginning of a line.
exec 2>&1

BIN=$(dirname "$0")
BIN=$(cd "${BIN}"; pwd)
HADOOP_CMD="${BIN}/hadoop"

TASK_TRACKER_HTTP_ADDRESS=$(${HADOOP_CMD} jobtracker -dumpConfiguration 2>/dev/null \
    | sed -n 's/.*task\.tracker\.http\.address","value":"\([.:0-9]*\)".*/\1/p')

if [[ -n "${TASK_TRACKER_HTTP_ADDRESS}" ]]; then
  curl -sm 10 -o /dev/null ${TASK_TRACKER_HTTP_ADDRESS}
  ERROR_CODE=$?
  if (( ${ERROR_CODE} == 28 )); then
    echo "ERROR curl timed out trying to reach the TaskTracker web server." \
        "Assuming the TaskTracker is unhealthy."
  elif (( ${ERROR_CODE} )); then
    echo "WARN curl failed to reach the TaskTracker, but did not time out."
  else
    echo "DEBUG Successfully curled TaskTracker."
  fi
else
  echo "WARN Failed to determine TaskTracker http address." \
      "Not checking health."
fi

# TaskTracker disregards ERRORs with non-zero exit code.
exit 0
