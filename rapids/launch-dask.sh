#!/bin/bash

# This script creates systemd configuration for Dask.

set -euxo pipefail

echo "Installing Dask service..."

readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_WORKER=$(/usr/share/google/get_metadata_value attributes/master-worker || echo -n 'true')
readonly DASK_LAUNCHER='/usr/local/bin/launch-dask.sh'
readonly INIT_SCRIPT='/usr/lib/systemd/system/dask-cluster.service'

cat << EOF > "${DASK_LAUNCHER}"
#!/bin/bash
if [[ "${ROLE}" == 'Master' ]]; then
  if [[ "${MASTER_WORKER}" == true ]]; then
    echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log.."
    /opt/conda/bin/dask-scheduler > /var/log/dask-scheduler.log 2>&1 &

    echo "dask-cuda-worker starting, logging to /var/log/dask-cuda-workers.log.."
    /opt/conda/bin/dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-workers.log 2>&1
  else
    echo "dask-scheduler starting, logging to /var/log/dask-scheduler.log.."
    /opt/conda/bin/dask-scheduler > /var/log/dask-scheduler.log 2>&1
  fi
else
  /opt/conda/bin/dask-cuda-worker --memory-limit 0 ${MASTER}:8786 > /var/log/dask-cuda-workers.log 2>&1
fi
EOF
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

echo "Dask cluster instantiation successful" >&2
