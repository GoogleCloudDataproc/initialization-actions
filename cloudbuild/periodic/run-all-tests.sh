#!/bin/bash

set -uxo pipefail

TESTS_TO_RUN=(
  "//hue:test_hue",
  "//datalab:test_datalab",
)

# ":DataprocInitActionsTestSuite"

bazel test \
	--jobs=15 \
	--local_cpu_resources=15 \
	--local_ram_resources=$((15 * 1024)) \
	--action_env=INTERNAL_IP_SSH=true \
	--test_output=errors \
	--noshow_progress \
	--noshow_loading_progress \
	--test_arg="--image_version=${IMAGE_VERSION}" \
	"${TESTS_TO_RUN[@]}"

ls
ls bazel-init-actions -R
ls bazel-bin -R
ls bazel-out -R
ls bazel-testlogs -R

COMMIT=$(git rev-parse HEAD)

get_build_num() {
	build_num=$(($(gsutil cat gs://init-actions-github-tests/counter.txt)-1))
	echo $build_num
}

BUILD_NUM=$(get_build_num)

# Reads the test logs of a given component by compiling all the logs of all its shards
get_test_logs() {
	component=$1
  SHARD_PATHS=(bazel-testlogs/${component}/test_${component}/shard*)
  #SHARD_DIRS=("${SHARD_PATHS[@]##*/}")
  all_logs=""
  for shard in "${SHARD_PATHS[@]}"; do
    logs_filepath=$(readlink -f ${shard}/test.log)
    logs=$(cat $logs_filepath)
    all_logs="${all_logs} \n\n ${logs}"
  done
	echo -e $all_logs
}

get_test_status() {
	component=$1
  SHARD_PATHS=(bazel-testlogs/${component}/test_${component}/shard*)
  #SHARD_DIRS=("${SHARD_PATHS[@]##*/}")
  declare -a test_results=()
  for shard in "${SHARD_PATHS[@]}"; do
  	xml_filepath=$(readlink -f ${shard}/test.xml)
    xml=$(cat $xml_filepath)
    # Parse the XML and determine if test passed or failed
    failures=$(xmllint --xpath 'string(/testsuites/@failures)' test.xml)
    errors=$(xmllint --xpath 'string(/testsuites/@errors)' test.xml)
    if [ "$errors" == "0" ] && [ "$failures" == "0" ]; then
      test_results+=("SUCCESS")
    else
      test_results+=("FAILURE")
    fi
  done
  # If there's a single test shard that fails, the test is considered a failure.
  if [[ " ${test_results[@]} " =~ "FAILURE" ]]; then
    status="FAILURE"
  else
    status="SUCCESS"
  fi
	echo $status
}

create_finished_json() {
  component=$1
  status=$(get_test_status $component)
  jq -n \
  	--argjson timestamp $(date +%s) \
  	--arg result $status \
  	--arg component $component \
  	--arg version $IMAGE_VERSION \
  	--arg build_num $BUILD_NUM \
  	--arg build_id $BUILD_ID \
  	--arg commit $COMMIT \
  	'{"timestamp":$timestamp, "result":$result, "job-version":$commit, "metadata": {"component":$component, "version":$version, "build_num":$build_num, "build_id":$build_id}}' > finished.json
}

shopt -s nullglob
COMPONENT_DIRS=(bazel-testlogs/*)
COMPONENT_DIRS=("${COMPONENT_DIRS[@]##*/}") # Get only the component name
for dir in "${COMPONENT_DIRS[@]}"; do
  # Create build-log.txt
  echo $(get_test_logs $dir) > build-log.txt

  # Create finished.json
  create_finished_json $dir

  output_dir="${dir}-${IMAGE_VERSION}"

  # Upload to GCS
  gsutil cp finished.json gs://init-actions-github-tests/logs/init_actions_tests/${output_dir}/${BUILD_NUM}/finished.json
  gsutil cp build-log.txt gs://init-actions-github-tests/logs/init_actions_tests/${output_dir}/${BUILD_NUM}/build-log.txt
  gsutil cp test.xml gs://init-actions-github-tests/logs/init_actions_tests/${output_dir}/${BUILD_NUM}/test.xml
done
