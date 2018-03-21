#!/bin/bash

set -euxo pipefail

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly DATAPROC_BUCKET="$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)"
readonly JUPYTER_AUTH_TOKEN="$(/usr/share/google/get_metadata_value attributes/JUPYTER_AUTH_TOKEN || true)"
readonly JUPYTER_PORT="$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT || echo 8123)"

# GCS directory in which to store notebooks.
# DATAPROC_BUCKET must not include gs:// and bucket must exist
readonly NOTEBOOK_DIR="${DATAPROC_BUCKET}/notebooks"

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_jupyter() {
  apt-get install -y python-dev python-tk
  easy_install pip
  pip install --upgrade jupyter jgscm matplotlib
}

function configure_jupyter() {
  # Create storage path if it does not exist
  hadoop fs -mkdir -p "gs://${NOTEBOOK_DIR}"

  echo "Creating Jupyter config..."
  jupyter notebook --allow-root --generate-config -y

  cat << EOF >> ~/.jupyter/jupyter_notebook_config.py

## Configs generated in Dataproc init action
c.Application.log_level = 'DEBUG'
c.JupyterApp.answer_yes = True
c.NotebookApp.ip = '*'
c.NotebookApp.allow_root= True
c.NotebookApp.open_browser = False
c.NotebookApp.port = ${JUPYTER_PORT}
c.NotebookApp.contents_manager_class = 'jgscm.GoogleStorageContentManager'
c.GoogleStorageContentManager.default_path = '${NOTEBOOK_DIR}'
c.NotebookApp.token = u'${JUPYTER_AUTH_TOKEN}'
EOF

  echo "Jupyter setup!"

  local init_script="/usr/lib/systemd/system/jupyter.service"

  cat << EOF > ${init_script}
[Unit]
Description=PySpark-based Jupyter Notebook Server
After=hadoop-yarn-resourcemanager.service

[Service]
Type=simple
Environment=PYSPARK_DRIVER_PYTHON=jupyter
Environment=PYSPARK_DRIVER_PYTHON_OPTS=notebook
ExecStart=/bin/bash -c '/usr/bin/pyspark &> /var/log/jupyter_notebook.log'
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

  chmod a+rw ${init_script}
}

function launch_jupyter() {
  systemctl daemon-reload
  systemctl enable jupyter
  systemctl restart jupyter
  systemctl status jupyter
}

function main() {
  if [[ "${ROLE}" == 'Master' ]]; then
    update_apt_get || err 'Failed to update apt-get'
    install_jupyter
    configure_jupyter
    launch_jupyter
    echo "Jupyter installation succeeded"
  fi
}

main

