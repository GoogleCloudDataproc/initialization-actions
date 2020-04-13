#!/bin/bash

set -uxo pipefail

# cleanup() {
# 	ls
# 	ls bazel-testlogs/hue
# }

# trap cleanup EXIT

echo "rand contents" > rand_file.txt
gsutil cp rand_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/rand_file.txt

TESTS_TO_RUN="//hue:test_hue" #":DataprocInitActionsTestSuite"

bazel test \
	--jobs=15 \
	--local_cpu_resources=15 \
	--local_ram_resources=$((15 * 1024)) \
	--action_env=INTERNAL_IP_SSH=true \
	--test_output=errors \
	--noshow_progress \
	--noshow_loading_progress \
	--test_arg="--image_version=${IMAGE_VERSION}" \
	"${TESTS_TO_RUN}"

ls
ls bazel-testlogs/hue/test_hue

# echo "rand contents" > rand_file.txt
# gsutil cp rand_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/rand_file.txt