#!/bin/bash

set -euxo pipefail

# build_num=$(($(gsutil cat gs://init-actions-github-tests/counter.txt)+1))
# echo "$build_num" > counter.txt
# gsutil cp counter.txt gs://init-actions-github-tests/counter.txt

shopt -s extglob;
DIRS=$(gsutil ls gs://init-actions-github-tests/logs/init_actions_tests/hue-1.4-debian9)
DIRS=("${DIRS[@]%/}") # Remove trailing "/" 
DIRS=("${DIRS[@]##*/}") # Get only the directory name (i.e. build number)
build_number="${DIRS[0]}" # Get latest build number

echo "$((${build_number}+1))" > /workspace/counter.txt