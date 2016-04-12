#!/usr/bin/env bash
set -e

DIR="${BASH_SOURCE%/*}"
[[ ! -d "$DIR" ]] && DIR="$PWD"

source "$DIR/../util/utils.sh"

function usage {
    echo "Creates an SSH tunnel and socks proxy and launches Chrome, using the environment "
    echo "variable DATAPROC_CLUSTER_NAME for the unique cluster name. The cluster metadata "
    echo "must contain a value for the key 'JUPYTER_PORT'."
    echo ""
    echo "If the appropriate environment variables are not set and the appropriate command"
    echo "line arguments are not given, then the usage message will be displayed and the "
    echo "script will exit."
    echo ""
    echo "usage: $0 [-h] [-c=cluster-name]"
    echo "    -h                 display help"
    echo "    -c=cluster-name    specify unique dataproc cluster name to launch"
    exit 1
}

for i in "$@"
do
    case $i in
        -c=*)
            DATAPROC_CLUSTER_NAME="${i#*=}"
            shift # past argument=value
            ;;
        -h)
            usage
            ;;
        *)
            ;;
    esac
done

[[ -z $DATAPROC_CLUSTER_NAME ]] && usage
JUPYTER_PORT=$(get_metadata_property $DATAPROC_CLUSTER_NAME JUPYTER_PORT || true)
[[ ! $JUPYTER_PORT =~ ^[0-9]+$ ]] && throw "metadata must contain a valid 'JUPYTER_PORT' value, but instead has the value \"$JUPYTER_PORT\""

# TODO: Ensure that Jupyter notebook is running on cluster master node

echo "Using following cluster name: $DATAPROC_CLUSTER_NAME"
echo "Using following remote dataproc jupyter port: $JUPYTER_PORT"
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
sleep 5 # Wait for tunnel to be ready before opening browser...

# 2.Launch Chrome instance, referencing the proxy server.
# TODO: Parameterize the chrome app path
# eval $CHROME_APP_PATH \
/Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \
    "http://$DATAPROC_CLUSTER_NAME-m:$JUPYTER_PORT" \
    --proxy-server="socks5://localhost:10000" \
    --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" \
    --user-data-dir=/tmp/

