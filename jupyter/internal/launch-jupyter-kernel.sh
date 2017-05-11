#!/usr/bin/env bash
set -e

echo "Installing Jupyter service..."

INIT_SCRIPT="/usr/lib/systemd/system/jupyter-notebook.service"

cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=Jupyter Notebook Server

[Service]
Type=simple
ExecStart=/bin/bash -c 'exec /opt/conda/bin/jupyter notebook --allow-root  --no-browser \
  &> /var/log/jupyter_notebook.log'

[Install]
WantedBy=multi-user.target
EOF

chmod a+rw ${INIT_SCRIPT}

echo "Starting Jupyter notebook..."

systemctl daemon-reload
systemctl enable jupyter-notebook
systemctl start jupyter-notebook

echo "Jupyter installation succeeded" >&2
