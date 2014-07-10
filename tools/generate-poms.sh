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
# Generates pom.xml-hadoop1 and pom.xml-hadoop2 for use in building
# POM files that have hadoop1 or hadoop2 baked into the version string. The
# GCS connector can then be built using
# mvn -Phadoop2 package -f pom.xml-hadoop2
#
# This script should be invoked as
# tools/generate-poms.sh <version>
#
# This script is based off of the hbase project's generate-hadoopX-poms.sh

set -o errexit

function print_usage()
{
  echo ""
  echo "Usage: tools/generate.sh <currentVersion>"
  echo "  currentVersion - The current version contained in the root pom.xml"
}

function generate_pom_files()
{
  hadoop_specific_version=$1
  hadoop_specific_pom=$2

  pom_files=$(find ./ -name pom.xml)

  for pom_file in $pom_files; do
    full_hadoop_specific_pom_path="$(dirname $pom_file)/${hadoop_specific_pom}"

    echo "Generating POM $full_hadoop_specific_pom_path"

    sed -e "s/<version>${VERSION}/<version>$hadoop_specific_version/" \
        -e "s/\(<module>[^<]*\)/\1\/${hadoop_specific_pom}/" \
        -e "s/\(relativePath>\.\.\)/\1\/${hadoop_specific_pom}/" \
        $pom_file > $full_hadoop_specific_pom_path

  done
}

declare -r VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Current version not provided."
  print_usage
  exit 1
fi

# Sanity check version:
if ! grep -q -e "<version>$VERSION</version>" pom.xml; then
  echo "Failed to find version $VERSION in pom.xml"
  exit 1
fi

declare -r HADOOP1_POM_NAME="pom.xml-hadoop1"
declare -r HADOOP1_VERSION="$VERSION-hadoop1"
declare -r HADOOP2_POM_NAME="pom.xml-hadoop2"
declare -r HADOOP2_VERSION="$VERSION-hadoop2"

generate_pom_files $HADOOP1_VERSION $HADOOP1_POM_NAME
generate_pom_files $HADOOP2_VERSION $HADOOP2_POM_NAME
