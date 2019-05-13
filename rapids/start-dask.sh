#!/usr/bin/env bash

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_WORKER=$(/usr/share/google/get_metadata_value attributes/master-worker || echo -n 'true')

if [[ "${ROLE}" == 'Master' ]]; then
  # master runs the scheduler
  dask-scheduler > /var/log/dask-scheduler.log 2>&1 &
  echo "dask-scheduler started, logging to /var/log/dask-scheduler.log.."

  # master also runs worker by defaults
  if [[ "${MASTER_WORKER}" == true ]]; then
    dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-workers.log 2>&1 &
    echo "dask-cuda-workers started, logging to /var/log/dask-cuda-workers.log.."
  fi
else
  # workers only run worker processes
  dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-workers.log 2>&1 &
  echo "dask-cuda-workers started, logging to /var/log/dask-cuda-workers.log.."
fi
