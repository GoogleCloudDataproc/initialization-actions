#!/bin/bash

set -euxo pipefail

build_num=$(($(gsutil cat gs://init-actions-github-tests/counter.txt)+1))
echo "$build_num" > counter.txt
gsutil cp counter.txt gs://init-actions-github-tests/counter.txt 