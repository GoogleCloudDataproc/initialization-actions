#!/usr/bin/env bash

num_workers=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
master_hostname="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
yarn_session="$1"
hostname="$(hostname)"
echo "---------------------------------"
echo "Starting validation on ${hostname} ${yarn_session}"

if [[ ${num_workers} == 0 ]] && [[ "${hostname}" == "${master_hostname}" ]]; then
  echo "  Single node detected - using transient cluster"
  echo "  Running flink job"
  HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run \
    -m yarn-cluster \
    -yn 1 -yjm 1024 -ytm 1024 \
    /usr/lib/flink/examples/batch/WordCount.jar
  flink_return_code=$?
  echo "  Done. Flink return code: ${flink_return_code}"
else
  if [[ ${yarn_session} == "False" ]]; then
    echo "  Running flink job because detected yarn session start false"
    HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run \
      -m yarn-cluster \
      -yn 1 -yjm 1024 -ytm 1024 \
      -p 2 \
      /usr/lib/flink/examples/batch/WordCount.jar
    yarn_result_code=$?
    echo "Yarn test result is ${yarn_result_code}"
  else
    echo 'flink-start-yarn-session set to true, so skipped validation for this case'
    yarn_result_code=0
    echo "  Using existing YARN session"
    application_id=$(yarn --loglevel ERROR application -list | grep -oP "(application_\d+_\d+)") &>/dev/null
    echo "  Running flink job"
    HADOOP_CONF_DIR=/etc/hadoop/conf /usr/lib/flink/bin/flink run \
      -m yarn-cluster \
      -yid ${application_id} \
      /usr/lib/flink/examples/batch/WordCount.jar

    flink_return_code=$?
    echo "  Done. Flink return code: ${flink_return_code}"
  fi
fi

if [[ ${flink_return_code} -eq 0 && ${yarn_result_code} -eq 0 ]]; then
  echo ""
  echo "Tests passed"
  echo ""
else
  echo "Tests failed"
fi
echo "---------------------------------"
