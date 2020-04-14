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

ls bazel-init-actions -R
ls bazel-bin -R
ls bazel-out -R
ls bazel-testlogs -R
cat bazel-out/k8-fastbuild/testlogs/hue/test_hue/shard_1_of_3/test.log
echo $(readlink -f bazel-testlogs/hue/test_hue/shard_1_of_3/test.cache_status) > cache_file.txt
gsutil cp cache_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/cache_file.txt
echo $(readlink -f bazel-testlogs/hue/test_hue/shard_1_of_3/test.log) > test_file.txt
gsutil cp test_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/test_file.txt
#ls bazel-testlogs/hue/test_hue


# echo "rand contents" > rand_file.txt
# gsutil cp rand_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/rand_file.txt