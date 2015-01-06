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



###############################################################################
# Runs the following jobs to validate a hadoop cluster; suitable for running
# over ssh automatically/without a terminal.
## teragen
## terasort
## teravalidate
# If they all pass 0 will be returned and 1 otherwise
################################################################################

# Usage:
#  ./bdutil shell < hadoop-validate-setup.sh
#

# Default to 10MB (100k records).
TERA_GEN_NUM_RECORDS=100000

# File hadoop-confg.sh
HADOOP_CONFIGURE_CMD=''
HADOOP_CONFIGURE_CMD=$(find ${HADOOP_LIBEXEC_DIR} ${HADOOP_PREFIX} \
    /home/hadoop /usr/*/hadoop* -name hadoop-config.sh | head -n 1)

# If hadoop-config.sh has been found source it
if [[ -n "${HADOOP_CONFIGURE_CMD}" ]]; then
  echo "Sourcing '${HADOOP_CONFIGURE_CMD}'"
  . ${HADOOP_CONFIGURE_CMD}
fi

#set the hadoop command and the path to the hadoop examples jar
HADOOP_CMD=$(which hadoop)
HADOOP_CMD="${HADOOP_CMD:-${HADOOP_PREFIX}/bin/hadoop}"

#find the hadoop examples jar
HADOOP_EXAMPLES_JAR=''

#find under HADOOP_PREFIX (tar ball install)
HADOOP_EXAMPLES_JAR=$(find ${HADOOP_PREFIX} -name 'hadoop-*examples*.jar' | grep -v source | head -n1)

#if its not found look under /usr/*/hadoop* (rpm/deb installs)
if [[ "${HADOOP_EXAMPLES_JAR}" == '' ]]; then
  HADOOP_EXAMPLES_JAR=$(find /usr/*/hadoop* -name 'hadoop-*examples*.jar' | grep -v source | head -n1)
fi

#if still not found, look under /usr/*/current/hadoop-mapreduce-client
if [[ "${HADOOP_EXAMPLES_JAR}" == '' ]]; then
  HADOOP_EXAMPLES_JAR=$(find -H /usr/*/current/hadoop-mapreduce-client -name 'hadoop-*examples*.jar' | grep -v source | head -n1)
fi

#if it is still empty then dont run the tests
if [[ "${HADOOP_EXAMPLES_JAR}" == '' ]]; then
  echo "Did not find hadoop-*examples-*.jar'"
  exit 1
fi

#dir where to store the data on hdfs. The data is relative of the users home dir on hdfs.
PARENT_DIR="validate_deploy_$(date +%s)"
TERA_GEN_OUTPUT_DIR="${PARENT_DIR}/tera_gen_data"
TERA_SORT_OUTPUT_DIR="${PARENT_DIR}/tera_sort_data"
TERA_VALIDATE_OUTPUT_DIR="${PARENT_DIR}/tera_validate_data"
#tera gen cmd
TERA_GEN_CMD="${HADOOP_CMD} jar ${HADOOP_EXAMPLES_JAR} teragen ${TERA_GEN_NUM_RECORDS} ${TERA_GEN_OUTPUT_DIR}"
TERA_GEN_LIST_CMD="${HADOOP_CMD} fs -ls ${TERA_GEN_OUTPUT_DIR}"

#tera sort cmd
TERA_SORT_CMD="${HADOOP_CMD} jar ${HADOOP_EXAMPLES_JAR} terasort ${TERA_GEN_OUTPUT_DIR} ${TERA_SORT_OUTPUT_DIR}"
TERA_SORT_LIST_CMD="${HADOOP_CMD} fs -ls ${TERA_SORT_OUTPUT_DIR}"

#tera validate cmd
TERA_VALIDATE_CMD="${HADOOP_CMD} jar ${HADOOP_EXAMPLES_JAR} teravalidate ${TERA_SORT_OUTPUT_DIR} ${TERA_VALIDATE_OUTPUT_DIR}"
TERA_VALIDATE_LIST_CMD="${HADOOP_CMD} fs -ls ${TERA_VALIDATE_OUTPUT_DIR}"

echo 'Starting teragen....'

#run tera gen
echo ${TERA_GEN_CMD}
time eval ${TERA_GEN_CMD}
EXIT_CODE=$?
echo 'Listing teragen output...'
echo ${TERA_GEN_LIST_CMD}
eval ${TERA_GEN_LIST_CMD}
if [[ ${EXIT_CODE} -ne 0 ]]; then
  echo 'tera gen failed.'
  exit 1
fi
echo 'Teragen passed starting terasort....'


#run tera sort
echo ${TERA_SORT_CMD}
time eval ${TERA_SORT_CMD}
EXIT_CODE=$?
echo 'Listing terasort output...'
echo ${TERA_SORT_LIST_CMD}
eval ${TERA_SORT_LIST_CMD}
if [[ ${EXIT_CODE} -ne 0 ]]; then
  echo 'tera sort failed.'
  exit 1
fi
echo 'Terasort passed starting teravalidate....'

#run tera validate
echo ${TERA_VALIDATE_CMD}
time eval ${TERA_VALIDATE_CMD}
EXIT_CODE=$?
echo 'Listing teravalidate output...'
echo ${TERA_VALIDATE_LIST_CMD}
eval ${TERA_VALIDATE_LIST_CMD}
if [[ ${EXIT_CODE} -ne 0 ]]; then
  echo 'tera validate failed.'
  exit 1
fi
echo 'teragen, terasort, teravalidate passed.'

echo "Cleaning the data created by tests: ${PARENT_DIR}"

CLEANUP_CMD="${HADOOP_CMD} dfs -rmr -skipTrash ${PARENT_DIR}"
echo ${CLEANUP_CMD}
eval ${CLEANUP_CMD}
