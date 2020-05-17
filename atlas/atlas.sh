#!/usr/bin/env bash

set -euxo pipefail

source "/usr/local/share/google/dataproc/bdutil/bdutil_helpers.sh"

readonly ATLAS_HOME="/usr/lib/atlas/apache-atlas"
readonly ATLAS_CONFIG="/etc/atlas/atlas-application.properties"
readonly INIT_SCRIPT="/usr/lib/systemd/system/atlas.service"

readonly ATLAS_ADMIN_USERNAME="$(/usr/share/google/get_metadata_value attributes/ATLAS_ADMIN_USERNAME || echo '')"
readonly ATLAS_ADMIN_PASSWORD_SHA256="$(/usr/share/google/get_metadata_value attributes/ATLAS_ADMIN_PASSWORD_SHA256 || echo '')"
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly ADDITIONAL_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)

function retry_command() {
  local -r cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function check_prerequisites() {
  # check for Zookeeper
  echo stat | nc localhost 2181 || err 'Zookeeper not found'

  # check for HBase
  if [[ "${ROLE}" == 'Master' ]]; then
    run_with_retries "systemctl is-active hbase-master"
  else
    run_with_retries "systemctl is-active hbase-regionserver"
  fi

  # Systemd and port checking are not deterministic for HBase Master
  run_with_retries "echo create \'$(hostname)\',\'$(hostname)\' | hbase shell -n"
  run_with_retries "echo disable \'$(hostname)\' | hbase shell -n"
  run_with_retries "echo drop \'$(hostname)\' | hbase shell -n"

  # check for Solr
  curl 'http://localhost:8983/solr' || err 'Solr not found'

  if [[ -n "${ADDITIONAL_MASTER}" ]]; then
    # check for Kafka on HA
    ls /usr/lib/kafka &>/dev/null || err 'Kafka not found'
  fi
}

function install_atlas() {
  apt_get_install atlas
  # TODO: fix in the deb package
  ln -s "${ATLAS_HOME}"-?.?.? "${ATLAS_HOME}" || true
  rm -rf "${ATLAS_HOME}/conf"
  ln -s "/etc/atlas" "${ATLAS_HOME}/conf" || true
}

function configure_solr() {
  if [[ $(hostname) == "${MASTER}" ]]; then
    # configure Solr only on the one actual Master node
    runuser -l solr -s /bin/bash -c "/usr/lib/solr/bin/solr create -c vertex_index -d /etc/atlas/solr -shards 3"
    runuser -l solr -s /bin/bash -c "/usr/lib/solr/bin/solr create -c edge_index -d /etc/atlas/solr -shards 3"
    runuser -l solr -s /bin/bash -c "/usr/lib/solr/bin/solr create -c fulltext_index -d /etc/atlas/solr -shards 3"
  fi
}

function configure_atlas() {
  local zk_quorum
  zk_quorum=$(bdconfig get_property_value --configuration_file /etc/hbase/conf/hbase-site.xml \
    --name hbase.zookeeper.quorum 2>/dev/null)
  local zk_url_for_solr
  zk_url_for_solr="$(echo "${zk_quorum}" | sed 's/,/:2181\\\/solr,/g'):2181\\/solr"

  local cluster_name
  cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

  # Symlink HBase conf dir
  mkdir "${ATLAS_HOME}/hbase"
  ln -s "/etc/hbase/conf" "${ATLAS_HOME}/hbase/conf"

  # Configure Atlas
  sed -i "s/atlas.graph.storage.hostname=.*/atlas.graph.storage.hostname=${zk_quorum}/" ${ATLAS_CONFIG}
  sed -i "s/atlas.graph.storage.hbase.table=.*/atlas.graph.storage.hbase.table=atlas/" ${ATLAS_CONFIG}

  sed -i "s/atlas.rest.address=.*/atlas.rest.address=http:\/\/$(hostname):21000/" ${ATLAS_CONFIG}
  sed -i "s/atlas.audit.hbase.zookeeper.quorum=.*/atlas.audit.hbase.zookeeper.quorum=${zk_quorum}/" ${ATLAS_CONFIG}

  if [[ -n "${ADDITIONAL_MASTER}" ]]; then
    # Configure HA
    sed -i "s/atlas.server.ha.enabled=.*/atlas.server.ha.enabled=true/" ${ATLAS_CONFIG}
    sed -i "s/atlas.server.ha.zookeeper.connect=.*/atlas.server.ha.zookeeper.connect=${zk_quorum}/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.wait-searcher=.*/#atlas.graph.index.search.solr.wait-searcher=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-url=.*/atlas.graph.index.search.solr.zookeeper-url=${zk_url_for_solr}/" ${ATLAS_CONFIG}

    cat <<EOF >>${ATLAS_CONFIG}
atlas.server.ids=m0,m1,m2
atlas.server.address.m0=${cluster_name}-m-0:21000
atlas.server.address.m1=${cluster_name}-m-1:21000
atlas.server.address.m2=${cluster_name}-m-2:21000
atlas.server.ha.zookeeper.zkroot=/apache_atlas
atlas.client.ha.retries=4
atlas.client.ha.sleep.interval.ms=5000
EOF
  else

    # Disable Solr Cloud
    sed -i "s/atlas.graph.index.search.solr.mode=cloud/#atlas.graph.index.search.solr.mode=cloud/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-url=.*/#atlas.graph.index.search.solr.zookeeper-url=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-connect-timeout=.*/#atlas.graph.index.search.solr.zookeeper-connect-timeout=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-session-timeout=.*/#atlas.graph.index.search.solr.zookeeper-session-timeout=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.wait-searcher=.*/#atlas.graph.index.search.solr.wait-searcher=.*/" ${ATLAS_CONFIG}

    # Enable Solr HTTP
    sed -i "s/#atlas.graph.index.search.solr.mode=http/atlas.graph.index.search.solr.mode=http/" ${ATLAS_CONFIG}
    sed -i "s/#atlas.graph.index.search.solr.http-urls=.*/atlas.graph.index.search.solr.http-urls=http:\/\/${MASTER}:8983\/solr/" ${ATLAS_CONFIG}

  fi

  # Override default admin username:password
  if [[ -n "${ATLAS_ADMIN_USERNAME}" && -n "${ATLAS_ADMIN_PASSWORD_SHA256}" ]]; then
    sed -i "s/admin=.*/${ATLAS_ADMIN_USERNAME}=ROLE_ADMIN::${ATLAS_ADMIN_PASSWORD_SHA256}/" \
      "${ATLAS_HOME}/conf/users-credentials.properties"
  fi

  # Configure to use local Kafka
  if ls /usr/lib/kafka &>/dev/null; then
    sed -i "s/atlas.notification.embedded=.*/atlas.notification.embedded=false/" ${ATLAS_CONFIG}
    sed -i "s/atlas.kafka.zookeeper.connect=.*/atlas.kafka.zookeeper.connect=${zk_quorum}/" ${ATLAS_CONFIG}
    sed -i "s/atlas.kafka.bootstrap.servers=.*/atlas.kafka.bootstrap.servers=$(hostname):9092/" ${ATLAS_CONFIG}
  fi

}

function start_atlas() {
  # TODO: move to DEB package
  cat <<EOF >${INIT_SCRIPT}
[Unit]
Description=Apache Atlas

[Service]
Type=forking
ExecStart=${ATLAS_HOME}/bin/atlas_start.py
ExecStop=${ATLAS_HOME}/bin/atlas_stop.py
RemainAfterExit=yes
TimeoutSec=10m

[Install]
WantedBy=multi-user.target
EOF
  chmod a+rw ${INIT_SCRIPT}
  systemctl enable atlas
  systemctl start atlas || err 'Unable to start atlas service'
}

function wait_for_atlas_to_start() {
  # atlas start script exits prematurely, before atlas actually starts
  # thus wait up to 10 minutes until atlas is fully working
  wait_for_port "Atlas web server" localhost 21000
  local -r cmd='curl localhost:21000/api/atlas/admin/status'
  for ((i = 0; i < 60; i++)); do
    if eval "${cmd}"; then
      return 0
    fi
    sleep 20
  done
  return 1
}

function wait_for_atlas_becomes_active_or_passive() {
  cmd="sudo ${ATLAS_HOME}/bin/atlas_admin.py -u doesnt:matter -status 2>/dev/null" # public check, but some username:password has to be given
  for ((i = 0; i < 60; i++)); do
    status=$(eval "${cmd}")
    if [[ ${status} == 'ACTIVE' || ${status} == 'PASSIVE' ]]; then
      return 0
    fi
    sleep 10
  done
  return 1
}

function enable_hive_hook() {
  bdconfig set_property \
    --name 'hive.exec.post.hooks' \
    --value 'org.apache.atlas.hive.hook.HiveHook' \
    --configuration_file '/etc/hive/conf/hive-site.xml' \
    --clobber

  echo "export HIVE_AUX_JARS_PATH=${ATLAS_HOME}/hook/hive" >>/etc/hive/conf/hive-env.sh
  ln -s ${ATLAS_CONFIG} /etc/hive/conf/
}

function enable_hbase_hook() {
  bdconfig set_property \
    --name 'hbase.coprocessor.master.classes' \
    --value 'org.apache.atlas.hbase.hook.HBaseAtlasCoprocessor' \
    --configuration_file '/etc/hbase/conf/hbase-site.xml' \
    --clobber

  ln -s ${ATLAS_HOME}/hook/hbase/* /usr/lib/hbase/lib/
  ln -s ${ATLAS_CONFIG} /etc/hbase/conf/

  sudo service hbase-master restart
}

function enable_sqoop_hook() {
  if [[ ! -f /usr/lib/sqoop ]]; then
    echo 'Sqoop not found, not configuring hook'
    return
  fi

  if [[ ! -f /usr/lib/sqoop/conf/sqoop-site.xml ]]; then
    cp /usr/lib/sqoop/conf/sqoop-site-template.xml /usr/lib/sqoop/conf/sqoop-site.xml
  fi

  bdconfig set_property \
    --name 'sqoop.job.data.publish.class' \
    --value 'org.apache.atlas.sqoop.hook.SqoopHook' \
    --configuration_file '/usr/lib/sqoop/conf/sqoop-site.xml' \
    --clobber

  ln -s ${ATLAS_HOME}/hook/sqoop/* /usr/lib/sqoop/lib
  ln -s ${ATLAS_CONFIG} /usr/lib/sqoop/conf

  sudo service hbase-master restart
}

function main() {
   if ! is_version_at_least "${DATAPROC_VERSION}" "1.5"; then
    err "Dataproc ${DATAPROC_VERSION} is not supported"
  fi

  if [[ "${ROLE}" == 'Master' ]]; then
    check_prerequisites
    install_atlas
    configure_solr
    configure_atlas
    enable_hive_hook
    enable_hbase_hook
    enable_sqoop_hook
    start_atlas
    wait_for_atlas_to_start
    wait_for_atlas_becomes_active_or_passive
  fi
}

main
