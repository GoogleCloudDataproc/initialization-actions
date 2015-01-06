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

#
# This script is intended to be run from the unix command line
# on an instance with hive installed (and the hive executable
# available in the user PATH).
#
# It is assumed that the one has already run the shell script
# ngram_hdfs_load.sh which will have downloaded the associated
# ngram data and deposited it into HDFS under /user/hdpusr/ngrams/<ngram>
#
# This script will create a table ("1gram") and then load each
# file into a separate partition within the table.
#

set -o errexit
set -o nounset

# Select what to install
readonly SCRIPT_DIR=$(dirname $0)
source $SCRIPT_DIR/ngram_setup.sh

# Create the table if it does not already exist
hive << END_CREATE
  CREATE TABLE IF NOT EXISTS $NGRAMS (
      word STRING,
      year INT,
      instance_count INT,
      book_count INT
  )
  PARTITIONED BY (prefix STRING)
  ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
  STORED AS TEXTFILE
  ;
  EXIT
  ;
END_CREATE

# Get the list of files to put into the table
FILE_PATTERN=$(printf $SOURCE_FORMAT $NGRAMS "" "")
FILE_LIST=$($HDFS_CMD -ls $HDFS_DIR | grep $FILE_PATTERN | awk '{ print $8 }')
for filepath in $FILE_LIST; do
  filename=$(basename $filepath)
  prefix=${filename##$FILE_PATTERN}

  hive --silent << END_LOAD
    LOAD DATA INPATH '$HDFS_DIR/$filename'
    OVERWRITE INTO TABLE $NGRAMS
    PARTITION (prefix='$prefix')
    ;
    EXIT
    ;
END_LOAD
done

echo "Data loaded into hive table $NGRAMS"
