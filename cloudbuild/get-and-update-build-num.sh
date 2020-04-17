#!/bin/bash

set -euxo pipefail

build_num=$(($(gsutil cat gs://init-actions-github-tests/counter.txt)+1))
echo "$build_num" > /workspace/counter.txt
gsutil cp /workspace/counter.txt gs://init-actions-github-tests/counter.txt 