#!/bin/bash

set -euxo pipefail

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
DATAPROC_BUCKET=$(curl -f -s -H Metadata-Flavor:Google http://metadata/computeMetadata/v1/instance/attributes/dataproc-bucket)
JUPYTER_PORT=$(/usr/share/google/get_metadata_value attributes/JUPYTER_PORT || true)
[[ ! $JUPYTER_PORT =~ ^[0-9]+$ ]] && JUPYTER_PORT=8123
JUPYTER_AUTH_TOKEN=$(/usr/share/google/get_metadata_value attributes/JUPYTER_AUTH_TOKEN || true)

# Only run on master
if [[ "${ROLE}" != 'Master' ]]; then
  exit 0;
fi

function update_apt_get() {
  for ((i = 0; i < 10; i++)); do
    if apt-get update; then
      return 0
    fi
    sleep 5
  done
  return 1
}

update_apt_get
apt-get install -y python-dev python-tk
easy_install pip
pip install --upgrade jupyter jgscm matplotlib

# Must not include gs:// and bucket must exist
NOTEBOOK_DIR="$DATAPROC_BUCKET/notebooks"
hadoop fs -mkdir -p "gs://$NOTEBOOK_DIR"

echo "Creating Jupyter config..."
jupyter notebook --allow-root --generate-config -y
cat << EOF >> ~/.jupyter/jupyter_notebook_config.py

## Configs generated in Dataproc init action
c.Application.log_level = 'DEBUG'
c.JupyterApp.answer_yes = True
c.NotebookApp.ip = '*'
c.NotebookApp.allow_root= True
c.NotebookApp.open_browser = False
c.NotebookApp.port = $JUPYTER_PORT
c.NotebookApp.contents_manager_class = 'jgscm.GoogleStorageContentManager'
c.GoogleStorageContentManager.default_path = '$NOTEBOOK_DIR'
c.NotebookApp.token = u'$JUPYTER_AUTH_TOKEN'
EOF
echo "Jupyter setup!"

INIT_SCRIPT="/usr/lib/systemd/system/jupyter.service"
cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=PySpark-based Jupyter Notebook Server
After=hadoop-yarn-resourcemanager.service

[Service]
Type=simple
Environment=PYSPARK_DRIVER_PYTHON=jupyter
Environment=PYSPARK_DRIVER_PYTHON_OPTS=notebook
ExecStart=/bin/bash -c '/usr/bin/pyspark &> /var/log/jupyter_notebook.log'

[Install]
WantedBy=multi-user.target
EOF

chmod a+rw ${INIT_SCRIPT}

systemctl daemon-reload
systemctl enable jupyter
systemctl restart jupyter
systemctl status jupyter

echo "Jupyter installation succeeded"
