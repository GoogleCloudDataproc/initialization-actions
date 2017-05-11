#!/usr/bin/env bash
set -e

echo "Installing Jupyter service..."

# Create a separate runner file to make it easier to pull in the right
# environment variables, etc,. before launching the notebook.
JUPYTER_LAUNCHER='/usr/local/bin/launch_jupyter.sh'

cat << EOF > ${JUPYTER_LAUNCHER}
#!/bin/bash

source /etc/profile.d/conda.sh
/opt/conda/bin/jupyter notebook --allow-root --no-browser
EOF
chmod 750 ${JUPYTER_LAUNCHER}

INIT_SCRIPT="/usr/lib/systemd/system/jupyter-notebook.service"

cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=Jupyter Notebook Server

[Service]
Type=simple
ExecStart=/bin/bash -c 'exec ${JUPYTER_LAUNCHER} \
    &> /var/log/jupyter_notebook.log'

[Install]
WantedBy=multi-user.target
EOF

chmod a+rw ${INIT_SCRIPT}

echo "Starting Jupyter notebook..."

systemctl daemon-reload
systemctl enable jupyter-notebook
systemctl restart jupyter-notebook
systemctl status jupyter-notebook

echo "Jupyter installation succeeded" >&2
