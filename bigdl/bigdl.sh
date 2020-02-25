#!/bin/bash

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

set -euxo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
WORKER_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
BIGDL_DOWNLOAD_URL=$(/usr/share/google/get_metadata_value attributes/bigdl-download-url || echo 'https://repo1.maven.org/maven2/com/intel/analytics/bigdl/dist-spark-2.3.1-scala-2.11.8-all/0.7.2/dist-spark-2.3.1-scala-2.11.8-all-0.7.2-dist.zip')

mkdir -p /opt/intel-bigdl
cd /opt/intel-bigdl

wget -nv --timeout=30 --tries=5 --retry-connrefused "${BIGDL_DOWNLOAD_URL}"
unzip -q ./*.zip

JAR=$(realpath lib/*.jar)
PYTHON_ZIP=$(realpath lib/*.zip)

cat <<EOF >>/etc/spark/conf/spark-env.sh
SPARK_DIST_CLASSPATH="\$SPARK_DIST_CLASSPATH:$JAR"
PYTHONPATH="\$PYTHONPATH:$PYTHON_ZIP"
EOF

# Config changes only need to happen on the master.
# This way we also avoid DOSing the resource manager when running yarn node -list

if [[ "${ROLE}" == "Master" ]]; then
  NUM_NODEMANAGERS_TARGET="${WORKER_COUNT}"
  if (("${WORKER_COUNT}" == 0)); then
    # Single node clusters have one node manager
    NUM_NODEMANAGERS_TARGET=1
  fi
  # Wait for 5 minutes for Node Managers to register and run.
  # Break early if the expected number of node managers have registered.
  for ((i = 0; i < 5 * 60; i++)); do
    CURRENTLY_RUNNING_NODEMANAGERS=$(yarn node -list | grep RUNNING | wc -l)
    if ((CURRENTLY_RUNNING_NODEMANAGERS == NUM_NODEMANAGERS_TARGET)); then
      break
    fi
    sleep 1
  done
  if ((CURRENTLY_RUNNING_NODEMANAGERS == 0)); then
    echo "No node managers running. Cluster creation likely failed"
    exit 1
  fi

  ## N.B: BigDL requires that we provide the exact number of executors (no dynamic allocation)
  # for optimizations related to the Intel MKL library
  # This calculation assumes:
  # 1) Node managers will never go away (e.g. no preemptible VMs and no cluster scaling)
  # 2) This cluster will only be used by one application at a time
  # 3) YARN vCores matches the number of vCores actually available on that node. (Sometimes users
  #    lie to YARN about the number of cores to run more (IO-bound) tasks in parallel
  # 4) Node manager cores / Spark executor cores is a correct approximation of the number
  #    of spark executors that can run on a node. Note that we configure YARN to schedule containers
  #    by memory request and ignore the vCore request.
  #
  # tl;dr this init action will work with default Dataproc configuration, but may break if users
  # provide other config properties.

  CORES_PER_NODEMANAGER=$(yarn node -list | tail -n 1 | cut -f 1 -d ' ' | xargs yarn node -status | perl -ne '/CPU-Capacity : (\d+) vcores/ && print "$1\n";')
  SPARK_EXECUTOR_CORES=$(grep spark.executor.cores /etc/spark/conf/spark-defaults.conf | tail -n 1 | sed 's/.*[[:space:]=]\+\([[:digit:]]\+\).*/\1/')
  SPARK_NUM_EXECUTORS_PER_NODE_MANAGER=$((CORES_PER_NODEMANAGER / SPARK_EXECUTOR_CORES))
  # Subtract one to account for the app master.
  # Also use CURRENTLY_RUNNING_NODEMANAGERS rather than WORKER_COUNT as the number of workers.
  # If running NMs < worker count, it likely means some workers failed to initialize and
  # will never come up.
  SPARK_NUM_EXECUTORS=$((SPARK_NUM_EXECUTORS_PER_NODE_MANAGER * CURRENTLY_RUNNING_NODEMANAGERS - 1))

  # Check if it BigDL conf or Zoo
  if [ -f conf/spark-bigdl.conf ]; then
    cat conf/spark-bigdl.conf >>/etc/spark/conf/spark-defaults.conf
  elif [ -f conf/spark-analytics-zoo.conf ]; then
    cat conf/spark-analytics-zoo.conf >>/etc/spark/conf/spark-defaults.conf
  else
    err "Can't find any suitable spark config for Intel BigDL/Zoo"
  fi

  cat <<EOF >>/etc/spark/conf/spark-defaults.conf

spark.dynamicAllocation.enabled=false
spark.executor.instances=${SPARK_NUM_EXECUTORS}

EOF

fi
