#!/bin/bash

set -euxo pipefail

shopt -s extglob;
DIRS=$(gsutil ls gs://init-actions-github-tests/logs/init_actions_tests/hue-1.4-debian9)
DIRS=("${DIRS[@]%/}") # Remove trailing "/" 
DIRS=("${DIRS[@]##*/}") # Get only the directory name (i.e. build number)
build_number="${DIRS[0]}" # Get latest build number

echo "$((${build_number}+1))" > counter.txt