#!/usr/bin/env bash
set -euxo pipefail

readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
readonly NUM_WORKERS=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly ZOOKEEPER_HOME=/usr/lib/zookeeper

readonly PULSAR_FUNCTIONS_ENABLED="$(/usr/share/google/get_metadata_value attributes/pulsar-functions-enabled || echo 'false')"
readonly BUILTIN_CONNECTORS_ENABLED="$(/usr/share/google/get_metadata_value attributes/builtin-connectors-enabled || echo 'false')"
readonly TIERED_STORAGE_OFFLOADERS_ENABLED="$(/usr/share/google/get_metadata_value attributes/tiered-storage-offloaders-enabled || echo 'false')"

readonly PULSAR_VERSION="$(/usr/share/google/get_metadata_value attributes/pulsar-version || echo '2.6.0')"
readonly PULSAR_HOME="/opt/apache-pulsar-${PULSAR_VERSION}"

readonly WEB_SERVICE_URL_PORT="$(/usr/share/google/get_metadata_value attributes/web-service-url-port || echo 8080)"               #default value, not recommended to change
readonly WEB_SERVICE_URL_TLS_PORT="$(/usr/share/google/get_metadata_value attributes/web-service-url-tls-port || echo 8443)"       #default value, not recommended to change
readonly BROKER_SERVICE_URL_PORT="$(/usr/share/google/get_metadata_value attributes/broker-service-url-port || echo 6650)"         #default value, not recommended to change
readonly BROKER_SERVICE_URL_TLS_PORT="$(/usr/share/google/get_metadata_value attributes/broker-service-url-tls-port || echo 6651)" #default value, not recommended to change

ZOOKEEPER_ADDRESSES=""
ZOOKEEPER_ADDRESS=""

WEB_SERVICE_URL=""
WEB_SERVICE_URL_TLS=""
BROKER_SERVICE_URL=""
BROKER_SERVICE_URL_TLS=""

function retry_command() {
  local retry_backoff=(1 1 2 3 5 8 13 21 34 55 89 144)
  local -a cmd=("$@")
  echo "About to run '${cmd[*]}' with retries..."

  local installation_succeeded=0
  for ((i = 0; i < ${#retry_backoff[@]}; i++)); do
    if eval "${cmd[@]}"; then
      installation_succeeded=1
      break
    else
      local sleep_time=${retry_backoff[$i]}
      echo "'${cmd[*]}' attempt $((i + 1)) failed! Sleeping ${sleep_time}." >&2
      sleep "${sleep_time}"
    fi
  done

  if ! ((installation_succeeded)); then
    echo "Final attempt of '${cmd[*]}'..."
    # Let any final error propagate all the way out to any error traps.
    "${cmd[@]}"
  fi
}

function edit() {
  if [ "$#" == "3" ]; then
    grep -q "$1" "$3" && sed -i "s/"$1"/"$2"/" "$3" || echo "$2" >>"$3"
  else
    echo "$1" >>"$2"
  fi
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function retry_apt_command() {
  cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function update_apt_get() {
  retry_apt_command "apt-get update"
}

function fetch_zookeeper_list() {
  local zookeeper_client_port
  zookeeper_client_port=$(grep 'clientPort' /etc/zookeeper/conf/zoo.cfg |
    tail -n 1 |
    cut -d '=' -f 2)

  local zookeeper_list
  zookeeper_list=$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg |
    cut -d '=' -f 2 |
    cut -d ':' -f 1 |
    sort |
    uniq |
    sed "s/$/:${zookeeper_client_port}/" |
    xargs echo |
    sed "s/ /,/g")

  ZOOKEEPER_ADDRESSES="${zookeeper_list}"

  # If attempt fails, error out.
  if [[ -z "${zookeeper_list}" ]]; then
    err 'Failed to find configured Zookeeper list. Please remember to include Zookeeper optional component'
  fi

  ZK_ADDRS="${zookeeper_list}"
  ZK_ADDRS=${ZK_ADDRS//,/$'\n'}
  ZK_ADDRS=($ZK_ADDRS)

  for i in "${!ZK_ADDRS[@]}"; do
    local serverNo=$i
    serverNo=$((serverNo + 1))
    edit "server.${serverNo}=${ZK_ADDRS[$i]}" conf/zookeeper.conf
  done

  ZOOKEEPER_ADDRESS="${zookeeper_list%%,*}"
}

function fetch_urls() {
  if [ "$NUM_WORKERS" == "0" ]; then
    WEB_SERVICE_URL+="$CLUSTER_NAME-w:$WEB_SERVICE_URL_PORT"
    WEB_SERVICE_URL_TLS+="$CLUSTER_NAME-w:$WEB_SERVICE_URL_TLS_PORT"
    BROKER_SERVICE_URL+="$CLUSTER_NAME-w:$BROKER_SERVICE_URL_PORT"
    BROKER_SERVICE_URL_TLS+="$CLUSTER_NAME-w:$BROKER_SERVICE_URL_TLS_PORT"
  else
    for ((i = 0; i < $NUM_WORKERS; i++)); do
      if [ "$i" == $(($NUM_WORKERS - 1)) ]; then
        WEB_SERVICE_URL+="$CLUSTER_NAME-w-$i:$WEB_SERVICE_URL_PORT"
        WEB_SERVICE_URL_TLS+="$CLUSTER_NAME-w-$i:$WEB_SERVICE_URL_TLS_PORT"
        BROKER_SERVICE_URL+="$CLUSTER_NAME-w-$i:$BROKER_SERVICE_URL_PORT"
        BROKER_SERVICE_URL_TLS+="$CLUSTER_NAME-w-$i:$BROKER_SERVICE_URL_TLS_PORT"
      else
        WEB_SERVICE_URL+="$CLUSTER_NAME-w-$i:$WEB_SERVICE_URL_PORT,"
        WEB_SERVICE_URL_TLS+="$CLUSTER_NAME-w-$i:$WEB_SERVICE_URL_TLS_PORT,"
        BROKER_SERVICE_URL+="$CLUSTER_NAME-w-$i:$BROKER_SERVICE_URL_PORT,"
        BROKER_SERVICE_URL_TLS+="$CLUSTER_NAME-w-$i:$BROKER_SERVICE_URL_TLS_PORT,"
      fi
    done
  fi
}

function install_and_configure_pulsar() {
  retry_command wget https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz -P /opt
  tar xvzf /opt/apache-pulsar-${PULSAR_VERSION}-bin.tar.gz

  mkdir /usr/lib/pulsar
  ln -s "$PULSAR_HOME" /usr/lib/pulsar

  cd /usr/lib/pulsar
  fetch_zookeeper_list
  fetch_urls

  configure_pulsar_client
}

function initialize_zookeeper_metadata() {
  bin/pulsar initialize-cluster-metadata \
    --cluster ${CLUSTER_NAME} \
    --zookeeper ${ZOOKEEPER_ADDRESS} \
    --configuration-store ${ZOOKEEPER_ADDRESS} \
    --web-service-url http://${WEB_SERVICE_URL} \
    --web-service-url-tls https://${WEB_SERVICE_URL_TLS} \
    --broker-service-url pulsar://${BROKER_SERVICE_URL} \
    --broker-service-url-tls pulsar+ssl://${BROKER_SERVICE_URL_TLS}
}

function wait_for_zookeeper() {
  for i in {1..20}; do
    if "${ZOOKEEPER_HOME}/bin/zkCli.sh" -server "${ZOOKEEPER_ADDRESS}" ls /; then
      return 0
    else
      echo "Failed to connect to ZooKeeper ${ZOOKEEPER_ADDRESS}, retry ${i}..."
      sleep 5
    fi
  done
  echo "Failed to connect to ZooKeeper ${ZOOKEEPER_ADDRESS}" >&2
  exit 1
}

function deploy_bookkeeper() {
  edit "zkServers=.*" "zkServers=${ZOOKEEPER_ADDRESSES}" "conf/bookkeeper.conf"
  bin/pulsar-daemon start bookie
}

function deploy_broker() {
  edit "zookeeperServers=.*" "zookeeperServers=${ZOOKEEPER_ADDRESSES}" "conf/broker.conf"
  edit "configurationStoreServers=.*" "configurationStoreServers=${ZOOKEEPER_ADDRESSES}" "conf/broker.conf"
  edit "clusterName=.*" "clusterName=${CLUSTER_NAME}" "conf/broker.conf"
  edit "brokerServicePort=.*" "brokerServicePort=${BROKER_SERVICE_URL_PORT}" "conf/broker.conf"
  edit "brokerServicePortTls=.*" "brokerServicePortTls=${BROKER_SERVICE_URL_TLS_PORT}" "conf/broker.conf"
  edit "webServicePort=.*" "webServicePort=${WEB_SERVICE_URL_PORT}" "conf/broker.conf"
  edit "webServicePortTls=.*" "webServicePortTls=${WEB_SERVICE_URL_TLS_PORT}" "conf/broker.conf"

  if [[ "${PULSAR_FUNCTIONS_ENABLED}" == true ]]; then
    edit "functionsWorkerEnabled=.*" "functionsWorkerEnabled=true" "conf/broker.conf"
    sed -i "s/pulsarFunctionsCluster: standalone/pulsarFunctionsCluster: ${CLUSTER_NAME}/" conf/functions_worker.yml
  fi

  if [[ "${BUILTIN_CONNECTORS_ENABLED}" == true ]]; then
    retry_command wget https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/connectors/{connector}-${PULSAR_VERSION}.nar
    mkdir connectors
    mv pulsar-io-aerospike-${PULSAR_VERSION}.nar connectors
  fi

  if [[ "${TIERED_STORAGE_OFFLOADERS_ENABLED}" == true ]]; then
    retry_command wget https://archive.apache.org/dist/pulsar/pulsar-${PULSAR_VERSION}/apache-pulsar-offloaders-${PULSAR_VERSION}-bin.tar.gz
    tar xvfz apache-pulsar-offloaders-${PULSAR_VERSION}-bin.tar.gz
    mv apache-pulsar-offloaders-${PULSAR_VERSION}/offloaders offloaders
  fi

  bin/pulsar-daemon start broker
}

function configure_pulsar_client() {
  edit "webServiceUrl=.*" "webServiceUrl=http:\/\/$WEB_SERVICE_URL" "conf/client.conf"
  edit "brokerServiceUrl=.*" "brokerServiceUrl=pulsar:\/\/$BROKER_SERVICE_URL" "conf/client.conf"
}

function main() {
  update_apt_get || err 'Unable to update packages lists.'
  # regardless of role, we want to install the pulsar binary.
  install_and_configure_pulsar
  wait_for_zookeeper
  initialize_zookeeper_metadata

  if [[ "${ROLE}" != 'Master' ]]; then
    deploy_bookkeeper
    deploy_broker
  fi
}

main
