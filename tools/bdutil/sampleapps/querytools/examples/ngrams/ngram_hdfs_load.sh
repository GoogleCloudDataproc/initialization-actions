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

# This script is intended to be run from the command line on the
# Hadoop master node by the hdpuser.
#
# $ ngram_hdfs_load.sh [N]
#
# where N defaults to 1 and indicates which Ngram dataset to download.
#
# The script will downlaod the zipped ngram files from a public
# Google Cloud Storage bucket to a local temporary directory,
# decompress the files, and then insert them into HDFS into
# hdfs:///user/hdpuser/ngrams/ directory.
#

set -o errexit
set -o nounset

# Select what to install
readonly SCRIPT_DIR=$(dirname $0)
source $SCRIPT_DIR/ngram_setup.sh

# Generate Partition List
PARTITION_LIST=$(echo {a..z})

for ((i = 1; i < N; i++)); do
  NEW_LIST=""
  for curr in $PARTITION_LIST; do
    curr=$(echo ${curr}{a..z})
    NEW_LIST="$NEW_LIST $curr"
  done
  PARTITION_LIST="$NEW_LIST"
done

# Start Downloads Into Stage_dir
mkdir -p $STAGE_DIR
echo "Building $NGRAMS download list"
SOURCE_LIST=""
for partition in $PARTITION_LIST; do
  filename_gz=$(printf $SOURCE_FORMAT $NGRAMS ${partition} ".gz")
  filename=${filename_gz%%.gz}

  stage_gz=$STAGE_DIR/$filename_gz
  stage=$STAGE_DIR/$filename

  # To make this restartable, we check what needs to be downloaded
  # before doing so...

  if [[ -e $stage_gz ]]; then
    echo "$filename_gz already downloaded"
    continue
  fi

  if [[ -e $stage ]]; then
    echo "$filename_gz downloaded and decompressed"
    continue
  fi

  if $HDFS_CMD -test -e $HDFS_DIR/$filename; then
    echo "HDFS: $HDFS_DIR/$filename already inserted"
    continue
  fi

  SOURCE_LIST="$SOURCE_LIST $filename_gz"
done

# Note that this process could be batched up into one call to
# gsutil -m, but in practice it had little performance impact
# and impacted the restartability (as partial files could be
# left in the stage dir).
if [[ -n $SOURCE_LIST ]]; then
  echo "Downloading $NGRAMS files from Cloud Storage to $STAGE_DIR"
  for filename_gz in $SOURCE_LIST; do
    remote=$SOURCE_LOCATION/$filename_gz
    stage_gz=$STAGE_DIR/$filename_gz

    gsutil cp $remote ${stage_gz}.tmp && \
      mv ${stage_gz}.tmp $stage_gz
  done
fi

# Decompress
set +o errexit
COMPRESSED_FILES=$(cd $STAGE_DIR && /bin/ls -1 *.gz 2>/dev/null)
set -o errexit
if [[ -n $COMPRESSED_FILES ]]; then
  echo "Decompressing $NGRAMS files in $STAGE_DIR"

  for filename in $COMPRESSED_FILES; do
    start_sec=$(date +%s)

    echo -n "Decompress $filename"
    gunzip $STAGE_DIR/$filename

    end_sec=$(date +%s)
    time_sec=$((end_sec - $start_sec))

    echo " in $time_sec seconds"
  done
fi

# Insert into HDFS
if ! $HDFS_CMD -test -e $HDFS_DIR; then
  $HDFS_CMD -mkdir $HDFS_DIR
fi

UNCOMPRESSED_FILES=$(cd $STAGE_DIR && /bin/ls -1 --ignore *.gz 2>/dev/null)
if [[ -n $UNCOMPRESSED_FILES ]]; then
  echo "Inserting $NGRAMS files into HDFS: $HDFS_DIR/"

  for filename in $UNCOMPRESSED_FILES; do
    start_sec=$(date +%s)

    echo -n "Insert to HDFS: $HDFS_DIR/$filename"
    $HDFS_CMD -put $STAGE_DIR/$filename $HDFS_DIR/${filename} && \
      rm $STAGE_DIR/$filename

    end_sec=$(date +%s)
    time_sec=$((end_sec - $start_sec))

    echo " in $time_sec seconds"
  done
fi

