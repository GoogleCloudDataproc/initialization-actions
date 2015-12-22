#!/bin/bash
set -e

# NOTE: Ensure that Jupyter notebook is running on cluster master node
# TODO: detect the project cluster name using gcloud tools
#DATAPROC_CLUSTER_NAME="spark-cluster"
#GCP_ZONE="us-east1-b"
#if [[ ! -v JUPYTER_PORT ]]; then
#    JUPYTER_PORT=8123
#fi

# 0. Set default path to Chrome application (by operating system type).
# OS X
CHROME_APP_PATH="/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome"
# Linux
#CHROME_APP_PATH="/usr/bin/google-chrome"
# Windows
#CHROME_APP_PATH="C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

# Following configuration at:
# https://cloud.google.com/dataproc/cluster-web-interfaces
# 1. Setup ssh tunnel and socks proxy
gcloud compute ssh  --ssh-flag="-D 1080" \
     --ssh-flag="-N" --ssh-flag="-n" "$DATAPROC_CLUSTER_NAME-m" &

# 2.Launch Chrome instance, referencing the proxy server.
# TODO: Parameterize the chrome app path
# eval $CHROME_APP_PATH \
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
    "http://$DATAPROC_CLUSTER_NAME-m:$JUPYTER_PORT" \
    --proxy-server="socks5://localhost:1080" \
    --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" \
    --user-data-dir=/tmp/

