#!/bin/bash

# This script creates systemd configuration for Dask.

set -euxo pipefail

echo "Installing Dask service..."

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly RUN_CUDA_WORKER_ON_MASTER=$(/usr/share/google/get_metadata_value attributes/run-cuda-worker-on-master || echo -n 'true')
readonly DASK_LAUNCHER='/usr/local/bin/launch-dask.sh'
readonly INIT_SCRIPT='/usr/lib/systemd/system/dask-cluster.service'
readonly PREFIX='/opt/conda/anaconda/envs/RAPIDS/bin'

if [[ "${ROLE}" == 'Master' ]]; then
    cat << EOF > "${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${RUN_CUDA_WORKER_ON_MASTER}" == true ]]; then
  echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log.."
  $PREFIX/dask-scheduler > /var/log/dask-scheduler.log 2>&1 &

  echo "dask-cuda-worker starting, logging to /var/log/dask-cuda-workers.log.."
  $PREFIX/dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-workers.log 2>&1
else
  echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log.."
  $PREFIX/dask-scheduler > /var/log/dask-scheduler.log 2>&1
fi
EOF
else
    cat << EOF > "${DASK_LAUNCHER}"
#!/bin/bash
$PREFIX/dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-workers.log 2>&1
EOF
fi
chmod 750 "${DASK_LAUNCHER}"

cat << EOF > "${INIT_SCRIPT}"
[Unit]
Description=Dask Cluster Service
[Service]
Type=simple
Restart=on-failure
ExecStart=/bin/bash -c 'exec ${DASK_LAUNCHER}'
[Install]
WantedBy=multi-user.target
EOF

chmod a+rw "${INIT_SCRIPT}"

echo "Starting Dask cluster..."

systemctl daemon-reload
systemctl enable dask-cluster
systemctl restart dask-cluster
systemctl status dask-cluster

echo "Dask cluster instantiation successful"
