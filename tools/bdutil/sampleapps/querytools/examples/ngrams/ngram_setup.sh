#!/bin/bash
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Utility script, sourced by both ngram_hdfs_load.sh and hive_table_create.sh
# This script will set a series of constants, some based on the choice
# of the command line "N" value (defaults to 1).  N indicates the ngram
# dataset to download and copy into HDFS.

readonly SOURCE_FORMAT="googlebooks-eng-all-%s-20120701-%s%s"
readonly SOURCE_LOCATION="gs://books/ngrams/books"

# The "hadoop" executable should be in the user path
readonly HDFS_CMD="hadoop fs"

# What to install: 1gram by default
N=1

# Now parse command line arguments
while [[ $# -ne 0 ]]; do
  case "$1" in
    --N=*)
      N=${1#--N=}
      shift
      ;;
    --help)
      N=
      shift
      ;;
    *)
  esac
done

if [[ ! $N -ge 1 ]]; then
  echo "usage $(basename $0): --N=<n>"
  exit 1
fi

# Now set constants based on the selection of N
readonly NGRAMS="${N}gram"
readonly HDFS_DIR="ngrams/$NGRAMS"
readonly STAGE_DIR="/hadoop/tmp/$USER/ngrams/$NGRAMS"

