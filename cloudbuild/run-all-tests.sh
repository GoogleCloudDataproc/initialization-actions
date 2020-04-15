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

get_stable_status() {
	stable_status_filepath=$(readlink -f bazel-out/stable-status.txt)
	stable_status=$(cat $stable_status_filepath)
	echo $stable_status
}

# Reads the test log of a given component
get_test_logs() {
	component=$1
	logs_filepath=$(readlink -f bazel-testlogs/${component}/test_${component}/shard_1_of_3/test.log)
	logs=$(cat $logs_filepath)
	echo $logs
}

get_test_xml() {
	component=$1
	xml_filepath=$(readlink -f bazel-testlogs/${component}/test_${component}/shard_1_of_3/test.xml)
    xml=$(cat $xml_filepath)
	echo $xml
}

cat bazel-out/k8-fastbuild/testlogs/hue/test_hue/shard_1_of_3/test.log

echo $(get_stable_status) > status_file.txt
gsutil cp status_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/status_file.txt

shopt -s nullglob
COMPONENT_DIRS=(bazel-testlogs/*)
COMPONENT_DIRS=("${COMPONENT_DIRS[@]##*/}") # Get only the component name
for dir in "${COMPONENT_DIRS[@]}"; do
  # Create started.json, finished.json and build-log.txt
  echo $(get_test_logs $dir) > build-log.txt
  echo $(get_test_xml $dir) > test.xml
  failures_num=$(grep -oP '(?<=failures=)"(.*?)"' "test.xml")
  echo $failures_num

  # Upload to GCS
  gsutil cp build-log.txt gs://init-actions-github-tests/logs/init_actions_tests/${BUILD_ID}/${dir}/build-log.txt
  gsutil cp test.xml gs://init-actions-github-tests/logs/init_actions_tests/${BUILD_ID}/${dir}/test.xml
done
#echo $(get_test_logs) > test_file.txt
#gsutil cp test_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/test_file.txt
#ls bazel-testlogs/hue/test_hue

