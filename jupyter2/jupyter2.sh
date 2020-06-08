#!/bin/bash

set -euxo pipefail

readonly NOT_SUPPORTED_MESSAGE="Jupyter 2 initialization action is not supported on Dataproc 2.0+.
Use Jupyter Component instead: https://cloud.google.com/dataproc/docs/concepts/components/jupyter"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

readonly DATAPROC_MASTER="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly DATAPROC_BUCKET="$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)"
readonly JUPYTER_AUTH_TOKEN="$(/usr/share/google/get_metadata_value attributes/JUPYTER_AUTH_TOKEN || true)"
readonly JUPYTER_PORT="$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT || echo 8123)"

# Place pyspark kernel.json file in /usr/local/share/jupyter (system-wide dir).
# Reference: https://ipython.readthedocs.io/en/latest/install/kernel_install.html
readonly KERNELSPEC_FILE=/usr/local/share/jupyter/kernels/pyspark/kernel.json

# GCS directory in which to store notebooks.
# DATAPROC_BUCKET must not include gs:// and bucket must exist
readonly NOTEBOOK_DIR="${DATAPROC_BUCKET}/notebooks"

# Systemd init script for jupyter
readonly INIT_SCRIPT="/usr/lib/systemd/system/jupyter.service"

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
  pip install --upgrade jupyter jgscm==0.1.7 matplotlib
}

function configure_jupyter() {
  # Create storage path if it does not exist
  hadoop fs -mkdir -p "gs://${NOTEBOOK_DIR}"

  echo "Creating Jupyter config..."
  jupyter notebook --allow-root --generate-config -y

  cat <<EOF >>~/.jupyter/jupyter_notebook_config.py

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

  echo "Creating kernelspec"
  mkdir -p "$(dirname ${KERNELSPEC_FILE})"
  # {connection_file} is a magic variable that Jupyter fills in for us
  # Note: we can only use it in argv, so cannot use env to set those
  # environment variables.
  cat <<EOF >"${KERNELSPEC_FILE}"
{
 "argv": [
    "bash",
    "-c",
    "PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS='kernel -f {connection_file}' pyspark"],
 "display_name": "PySpark",
 "language": "python"
}
EOF
  # Ensure Jupyter has picked up the new kernel
  jupyter kernelspec list | grep pyspark || err "Failed to create kernelspec"

  cat <<EOF >"${INIT_SCRIPT}"
[Unit]
Description=Jupyter Notebook Server
After=hadoop-yarn-resourcemanager.service

[Service]
Type=simple
ExecStart=/bin/bash -c 'jupyter notebook &> /var/log/jupyter_notebook.log'
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF

  chmod a+rw "${INIT_SCRIPT}"

  echo "Jupyter setup!"
}

function launch_jupyter() {
  systemctl daemon-reload
  systemctl enable jupyter
  systemctl restart jupyter
  systemctl status jupyter
}

function main() {
  # In an HA cluster, only run on -m-0
  if [[ "${HOSTNAME}" == "${DATAPROC_MASTER}" ]]; then
    update_apt_get || err 'Failed to update apt-get'
    install_jupyter
    configure_jupyter
    launch_jupyter
    echo "Jupyter installation succeeded"
  fi
}

main
