#!/bin/bash
#set -x
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

# This script installs Below Datasketches libraries on Dataproc cluster 2.1 and above
# datasketches-java - https://github.com/apache/datasketches-java 
# datasketches-memory - https://github.com/apache/datasketches-memory
# datasketches-hive - https://github.com/apache/datasketches-hive
# datasketches-pig - https://github.com/apache/datasketches-pig
# Official documentation link - https://datasketches.apache.org/

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

if [[ $(echo "${DATAPROC_IMAGE_VERSION} < 2.1" | bc -l) == 1  ]]; then
  echo "Datasketches integration is not supported on Dataproc image versions < 2.1"
  exit 0
fi

readonly MAVEN_CENTRAL_URI=https://maven-central.storage-download.googleapis.com/maven2
readonly DS_LIBPATH="/usr/lib/datasketches"
readonly SPARK_VERSION=$(spark-submit --version 2>&1 | sed -n 's/.*version[[:blank:]]\+\([0-9]\+\.[0-9]\).*/\1/p' | head -n1)
readonly SPARK_JAVA_EXAMPLE_JAR="gs://spark-lib/datasketches/spark-java-thetasketches-1.0-SNAPSHOT.jar"

function configure_libraries()
{
  local component=$1
  local version=$2
  wget -P "${DS_LIBPATH}" "${MAVEN_CENTRAL_URI}"/org/apache/datasketches/datasketches-"${component}"/"${version}"/datasketches-"${component}"-"${version}".jar
  if [ $? -eq 0 ]; then
    echo "Downloaded datasketches-"${component}"-"${version}".jar successfully"
  else
    echo "Problem downloading datasketches-"${component}"-"${version}".jar from ${MAVEN_CENTRAL_URI}, exiting!"
    exit 1
  fi
}

#Create datasketches lib directory
mkdir ${DS_LIBPATH}

declare -A all_components=( [java]="3.1.0" [hive]="1.2.0" [memory]="2.0.0" [pig]="1.1.0" )

#Download and configure datasketches libraries
for lib in "${!all_components[@]}" 
do
	configure_libraries $lib ${all_components[$lib]}
done

#Deploy spark java thetasketches example jar
if [[ "${SPARK_VERSION}" < "3.5" ]]; then
  gsutil cp "${SPARK_JAVA_EXAMPLE_JAR}" "${DS_LIBPATH}"
  if [ $? -eq 0 ]; then
    echo "Downloaded "${SPARK_JAVA_EXAMPLE_JAR}" successfully"
  else
    echo "Problem downloading "${SPARK_JAVA_EXAMPLE_JAR}" from GCS, exiting!"
  fi

else
  echo "Datasketches libraries are already included in Spark version 3.5.0 and onwards! Follow README for examples"
fi