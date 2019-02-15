#!/usr/bin/env bash
readonly DRILL_HOME=/usr/lib/drill
hostname="$(hostname)"
status=0

function sql_embedded(){
{
sudo ./bin/drill-embedded <<"EOF"
SELECT * FROM cp.`employee.json` LIMIT 3;
EOF
} > /dev/null
echo $?
}

function sql_distributed_local_zk(){
{
sudo ./bin/drill-localhost <<"EOF"
SELECT * FROM cp.`employee.json` LIMIT 3;
EOF
} > /dev/null
echo $?
}

function sql_distributed(){
{
sudo ./bin/drill-conf <<"EOF"
SELECT * FROM cp.`employee.json` LIMIT 3;
EOF
} > /dev/null
echo $?
}


echo '---------------------------------'
echo "Starting validation on ${hostname}"
echo "---------------------------------"

cd ${DRILL_HOME}
# waiting for 200 from Drill UI
(
while [[ ${status} != 200 ]]; do
  status=$(curl -s -o /dev/null -w "%{http_code}" localhost:8047)
  sleep 1
  if [[ ${status} == 200 ]]; then
    echo ${status} > file
  fi
  done
) &

# run embedded drill for non-zookeeper clusters
if [[ "$1" != "ZOOKEEPER" ]]; then
  echo "starting drill embedded for non-zookeeper cluster"
  ./bin/drill-embedded &
fi

while ! [[ -s file ]]; do
  echo "waiting"
  sleep 2
  done

ui_result=$(<file)

if [[ "$1" == "ZOOKEEPER" ]]; then
  if [[ "$2" == "${hostname}" ]]; then
    echo "Trying drill distributed with local zk"
    sql_result=$(sql_distributed_local_zk)
  else
    echo "Trying drill distributed connection to $2 using zookeeper"
    sql_result=$(sql_distributed)
  fi
# SINGLE
else
  echo "Trying drill-embedded, stopping current one"
  sudo ./bin/drillbit.sh stop
  sql_result=$(sql_embedded)
fi

echo "UI validation result is ${ui_result} and SQL validation result is ${sql_result}"
if (( ${ui_result} == 200 && ${sql_result} == 0 )); then
  echo 'Test passed'
  exit 0
else
  echo "Test failed"
  exit 1
fi



