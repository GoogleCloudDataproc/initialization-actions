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

# Runs a basic Pig script.
# Usage: ./bdutil shell < extensions/querytools/pig-validate-setup.sh

# File hadoop-confg.sh
HADOOP_CONFIGURE_CMD=''
HADOOP_CONFIGURE_CMD=$(find ${HADOOP_LIBEXEC_DIR} ${HADOOP_PREFIX} \
    /home/hadoop /usr/*/hadoop* -name hadoop-config.sh | head -n 1)

# If hadoop-config.sh has been found source it
if [[ -n "${HADOOP_CONFIGURE_CMD}" ]]; then
  echo "Sourcing '${HADOOP_CONFIGURE_CMD}'"
  . ${HADOOP_CONFIGURE_CMD}
fi

HADOOP_CMD=$(find ${HADOOP_PREFIX} /home/hadoop /usr/*/hadoop* -wholename '*/bin/hadoop' | head -n 1)
PIG_CMD=$(find ${HADOOP_PREFIX} /home/hadoop /usr/*/pig* -wholename '*/bin/pig' | head -n 1)

#if it is still empty then dont run the tests
if [[ "${HADOOP_CMD}" == '' ]]; then
  echo "Did not find hadoop'"
  exit 1
fi

#if it is still empty then dont run the tests
if [[ "${PIG_CMD}" == '' ]]; then
  echo "Did not find pig'"
  exit 1
fi

# Upload sample data.
PARENT_DIR="/validate_pig_$(date +%s)"
${HADOOP_CMD} fs -mkdir ${PARENT_DIR}
${HADOOP_CMD} fs -put /etc/passwd ${PARENT_DIR}

# Create a basic Pig script.
echo "Creating pigtest.pig..."
cat << EOF > pigtest.pig
SET job.name 'PigTest';
data = LOAD '${PARENT_DIR}/passwd'
      USING PigStorage(':')
      AS (user:CHARARRAY, dummy:CHARARRAY, uid:INT, gid:INT,
          name:CHARARRAY, home:CHARARRAY, shell:CHARARRAY);
grp = GROUP data BY (shell);
counts = FOREACH grp GENERATE
        FLATTEN(group) AS shell:CHARARRAY, COUNT(data) AS shell_count:LONG;
res = ORDER counts BY shell_count DESC, shell DESC;
DUMP res;
EOF
cat pigtest.pig

# Run the script.
${PIG_CMD} pigtest.pig > /tmp/pigoutput.txt

echo "Pig output:"
cat /tmp/pigoutput.txt

# Run an equivalent pipeline of command-line invocations which pull out the
# 'shell' field, sort/uniq to get the counts of each occurence, then finally
# format to match Pig by printing comma-separated fields in parens:
# (shell_count,shell)
cat /etc/passwd | awk -F: '{print $7}' | sort | uniq -c | sort -nr | \
    awk '{print $2, $1}' | sed "s/\(.*\) \(.*\)/(\1,\2)/" > /tmp/goldenoutput.txt

echo "Expected output:"
cat /tmp/goldenoutput.txt

EXIT_CODE=0
if diff /tmp/pigoutput.txt /tmp/goldenoutput.txt; then
  echo "Verified correct output."
else
  echo "Pig output doesn't match expected output!"
  EXIT_CODE=1
fi

# Cleanup.
echo "Cleaning up test data: ${PARENT_DIR}"
${HADOOP_CMD} fs -rmr -skipTrash ${PARENT_DIR}

exit ${EXIT_CODE}
