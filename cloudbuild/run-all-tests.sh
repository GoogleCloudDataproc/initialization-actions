#!/bin/bash

set -uxo pipefail


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

create_finished_json() {
  component=$1
  echo $(get_test_xml $component) > test.xml
  failures_num=$(grep -oP '(?<=failures=")(\d)' "test.xml")
  errors_num=$(grep -oP '(?<=errors=")(\d)' "test.xml")
  if [ "$errors_num" == "0" ] && [ "$failures_num" == "0" ]; then
  	status="SUCCESS"
  else
  	status="FAILED"
  fi
  jq -n \
  	--arg timestamp $(date +%s) \
  	--arg result $status \
  	'{"timestamp":$timestamp, "result":$result, "metadata": {}}' > finished.json
}

shopt -s nullglob
COMPONENT_DIRS=(bazel-testlogs/*)
COMPONENT_DIRS=("${COMPONENT_DIRS[@]##*/}") # Get only the component name
for dir in "${COMPONENT_DIRS[@]}"; do
  # Create build-log.txt
  echo $(get_test_logs $dir) > build-log.txt

  # Create finished.json
  create_finished_json $dir

  #Get build number
  build_num=$(($(gsutil cat gs://init-actions-github-tests/counter.txt)+1))
  echo "$build_num" > counter.txt
  gsutil cp counter.txt gs://init-actions-github-tests/counter.txt

  # Upload to GCS
  gsutil cp finished.json gs://init-actions-github-tests/logs/init_actions_tests/${build_num}/${dir}/finished.json
  gsutil cp build-log.txt gs://init-actions-github-tests/logs/init_actions_tests/${build_num}/${dir}/build-log.txt
  gsutil cp test.xml gs://init-actions-github-tests/logs/init_actions_tests/${build_num}/${dir}/test.xml
done
#echo $(get_test_logs) > test_file.txt
#gsutil cp test_file.txt gs://init-actions-github-tests/logs/init_actions_tests/1/test_file.txt
#ls bazel-testlogs/hue/test_hue

