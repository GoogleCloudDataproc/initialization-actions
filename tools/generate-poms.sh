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
# tools/generate-poms.sh
#
# This script is based off of the hbase project's generate-hadoopX-poms.sh

set -o errexit

function generate_pom_files()
{
  hadoop_specific_suffix=$1
  hadoop_specific_pom="pom.xml-${hadoop_specific_suffix}"

  pom_files=$(find ./ -name pom.xml)

  for pom_file in ${pom_files}; do
    full_hadoop_specific_pom_path="$(dirname ${pom_file})/${hadoop_specific_pom}"

    echo "Generating POM ${full_hadoop_specific_pom_path}"

    sed -e "/<artifactId>bigdataoss-parent<\/artifactId>/{n;
            s/\(<version>[^<]*\)/\1-${hadoop_specific_suffix}/;}" \
        -e "s/\(<module>[^<]*\)/\1\/${hadoop_specific_pom}/" \
        -e "s/\(relativePath>\.\.\)/\1\/${hadoop_specific_pom}/" \
        ${pom_file} > ${full_hadoop_specific_pom_path}

  done
}

generate_pom_files 'hadoop1'
generate_pom_files 'hadoop2'
