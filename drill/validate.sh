#!/usr/bin/env bash

set -Eeuxo pipefail

readonly DRILL_HOME=/usr/lib/drill
hostname="$(hostname)"
status=0

function sql_embedded() {
  {
    sudo ./bin/drill-embedded <<"EOF"
SELECT * FROM cp.`employee.json` LIMIT 3;
EOF
  } >/dev/null
  echo $?
}

function sql_distributed_local_zk() {
  {
    sudo ./bin/drill-localhost <<"EOF"
SELECT * FROM cp.`employee.json` LIMIT 3;
EOF
  } >/dev/null
  echo $?
}

function sql_distributed() {
  {
    sudo ./bin/drill-conf <<"EOF"
SELECT * FROM cp.`employee.json` LIMIT 3;
EOF
  } >/dev/null
  echo $?
}

echo '---------------------------------'
echo "Starting validation on ${hostname}"
echo "---------------------------------"

cd ${DRILL_HOME}

# Waiting for 200 from Drill UI
if [[ "$1" == "DISTRIBUTED" ]]; then
  (
    while [[ ${status} != 200 ]]; do
      sleep 3
      status=$(curl -s -o /dev/null -w "%{http_code}" localhost:8047)
      if [[ ${status} == 200 ]]; then
        echo "${status}" >drill.status
      fi
    done
  ) &
else
  # Skip UI testing in embedded (non-Zookeeper cluster) mode, because Drill UI is not running
  echo "200" >drill.status
fi

for i in {0..60}; do
  if [[ -s drill.status ]]; then
    break
  fi
  echo "Waiting on Drill UI..."
  sleep 5
done

if [[ ! -s drill.status ]]; then
  echo "Test failed: Drill UI did not initialize"
  exit 1
fi

ui_result=$(<drill.status)

if [[ "$1" == "DISTRIBUTED" ]]; then
  if [[ "$2" == "${hostname}" ]]; then
    echo "Trying drill distributed with local zk"
    sql_result=$(sql_distributed_local_zk)
  else
    echo "Trying drill distributed connection to $2 using zookeeper"
    sql_result=$(sql_distributed)
  fi
# SINGLE
else
  echo "Trying drill-embedded without zk"
  sql_result=$(sql_embedded)
fi

echo "UI validation result is ${ui_result} and SQL validation result is ${sql_result}"
if ((ui_result == 200 && sql_result == 0)); then
  echo 'Test passed'
  exit 0
fi

echo "Test failed"
exit 1
