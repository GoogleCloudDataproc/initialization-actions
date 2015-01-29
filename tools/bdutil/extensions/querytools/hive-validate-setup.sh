#!/usr/bin/env bash
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

# Runs a basic Hive script.
# Usage: ./bdutil shell < extensions/querytools/hive-validate-setup.sh

# File hadoop-confg.sh
HADOOP_CONFIGURE_CMD=''
HADOOP_CONFIGURE_CMD=$(find ${HADOOP_LIBEXEC_DIR} ${HADOOP_PREFIX} \
    /home/hadoop /usr/*/hadoop* /usr/*/current/hadoop* -name hadoop-config.sh | head -n 1)

# If hadoop-config.sh has been found source it
if [[ -n "${HADOOP_CONFIGURE_CMD}" ]]; then
  echo "Sourcing '${HADOOP_CONFIGURE_CMD}'"
  . ${HADOOP_CONFIGURE_CMD}
fi

HADOOP_CMD=$(find ${HADOOP_PREFIX} /home/hadoop /usr/*/hadoop* /usr/*/current/hadoop* -wholename '*/bin/hadoop' | head -n 1)
HIVE_CMD=$(find ${HADOOP_PREFIX} /home/hadoop /usr/*/hive* /usr/*/current/hive* -wholename '*/bin/hive' | head -n 1)

#if it is still empty then dont run the tests
if [[ "${HADOOP_CMD}" == '' ]]; then
  echo "Did not find hadoop'"
  exit 1
fi

#if it is still empty then dont run the tests
if [[ "${HIVE_CMD}" == '' ]]; then
  echo "Did not find hive'"
  exit 1
fi

# Upload sample data.
PARENT_DIR="/tmp/validate_hive_$(date +%s)"
${HADOOP_CMD} fs -mkdir ${PARENT_DIR}
${HADOOP_CMD} fs -put /etc/passwd ${PARENT_DIR}

# Create a basic Hive script.
echo "Creating hivetest.hive..."
cat << EOF > hivetest.hive
DROP TABLE bdutil_validate_hive_tbl;

CREATE TABLE bdutil_validate_hive_tbl (
  user STRING,
  dummy STRING,
  uid INT,
  gid INT,
  name STRING,
  home STRING,
  shell STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ':'
STORED AS TEXTFILE;

LOAD DATA INPATH '${PARENT_DIR}/passwd'
OVERWRITE INTO TABLE bdutil_validate_hive_tbl;

SELECT shell, COUNT(*) shell_count
FROM bdutil_validate_hive_tbl
GROUP BY shell
ORDER BY shell_count DESC, shell DESC;
EOF
cat hivetest.hive

# Run the script.
${HIVE_CMD} -f hivetest.hive > /tmp/hiveoutput.txt

echo "Hive output:"
cat /tmp/hiveoutput.txt

# Run an equivalent pipeline of command-line invocations which pull out the
# 'shell' field, sort/uniq to get the counts of each occurence, then finally
# format to match Hive by printing tab-separated fields:
# shell_count\tshell
cat /etc/passwd | awk -F: '{print $7}' | sort | uniq -c | sort -nr | \
    awk '{print $2, $1}' | sed "s/ /\t/" > /tmp/goldenoutput.txt

echo "Expected output:"
cat /tmp/goldenoutput.txt

EXIT_CODE=0
if diff /tmp/hiveoutput.txt /tmp/goldenoutput.txt; then
  echo "Verified correct output."
else
  echo "Hive output doesn't match expected output!"
  EXIT_CODE=1
fi

# Cleanup.
echo "Cleaning up test data: ${PARENT_DIR}"
${HADOOP_CMD} fs -rmr -skipTrash ${PARENT_DIR}

exit ${EXIT_CODE}
