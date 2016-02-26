#!/bin/bash
set -e

function usage {
    echo "Creates an SSH tunnel and socks proxy and launches Chrome, using the environment "
    echo "variables DATAPROC_CLUSTER_NAME and, if it's set, DATAPROC_JUPYTER_PORT for the "
    echo "unique cluster name and remote dataproc jupyter port, respectively."
    echo ""
    echo "If the appropriate environment variables are not set and the appropriate "
    echo "command line arguments are not given, then the usage message will "
    echo "be displayed and the script will exit."
    echo ""
    echo "usage: $0 [-h] [-c=cluster-name] [-p=dataproc-jupyter-port]"
    echo "	-h			display help"
    echo "	-c=cluster-name		specify unique dataproc cluster name to launch"
    echo "	-p=dataproc-jupyter-port	specify remote dataproc jupyter port (defaults to 8123)"
    exit 1
}

for i in "$@"
do
    case $i in
        -c=*)
            DATAPROC_CLUSTER_NAME="${i#*=}"
            shift # past argument=value
            ;;
        -p=*)
            DATAPROC_JUPYTER_PORT="${i#*=}"
            shift # past argument=value
            ;;
        -h)
            usage
            ;;
        *)
            ;;
    esac
done

DATAPROC_JUPYTER_PORT="${DATAPROC_JUPYTER_PORT:-8123}"

[[ -z ${DATAPROC_CLUSTER_NAME+x} ]] && usage

# TODO: Ensure that Jupyter notebook is running on cluster master node

echo "Using following cluster name: $DATAPROC_CLUSTER_NAME"
echo "Using following remote dataproc jupyter port: $DATAPROC_JUPYTER_PORT"
echo ""

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
gcloud compute ssh --ssh-flag="-D 10000" --ssh-flag="-N" --ssh-flag="-n" "$DATAPROC_CLUSTER_NAME-m" &

# 2.Launch Chrome instance, referencing the proxy server.
# TODO: Parameterize the chrome app path
# eval $CHROME_APP_PATH \
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
    "http://$DATAPROC_CLUSTER_NAME-m:$DATAPROC_JUPYTER_PORT" \
    --proxy-server="socks5://localhost:10000" \
    --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" \
    --user-data-dir=/tmp/

