#!/usr/bin/env bash

set -euxo pipefail

readonly ATLAS_VERSION='1.1.0'
readonly ATLAS_FILE="apache-atlas-${ATLAS_VERSION}-bin"
readonly ATLAS_GCS_URL="gs://dariusz-init-actions/atlas/${ATLAS_FILE}.tar.gz" # TODO(Aniszewski): change to official GCS location before merging
readonly ATLAS_HOME='/etc/atlas'
readonly ATLAS_CONFIG="${ATLAS_HOME}/conf/atlas-application.properties"
readonly ATLAS_TMP_TARBALL_PATH='/tmp/atlas-${ATLAS_VERSION}.tar.gz'
readonly INIT_SCRIPT='/usr/lib/systemd/system/atlas.service'
readonly ATLAS_ADMIN_USERNAME="$(/usr/share/google/get_metadata_value attributes/ATLAS_ADMIN_USERNAME)"
readonly ATLAS_ADMIN_PASSWORD_SHA256="$(/usr/share/google/get_metadata_value attributes/ATLAS_ADMIN_PASSWORD_SHA256)"
readonly MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly ADDITIONAL_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}


function check_prerequisites(){
  # check for Zookeeper
  echo stat | nc localhost 2181 || err 'Zookeeper not found'

  # check for HBase
  echo "list" | hbase shell || err 'HBase not found'

  # check for Solr
  curl 'http://localhost:8983/solr' || err 'Solr not found'

  if [[ -n "${ADDITIONAL_MASTER}" ]]; then
    # check for Kafka on HA
    ls /usr/lib/kafka &>/dev/null || err 'Kafka not found'
  fi
}

function download_atlas(){
  gsutil cp "${ATLAS_GCS_URL}" "${ATLAS_TMP_TARBALL_PATH}"
  tar -xf "${ATLAS_TMP_TARBALL_PATH}"
  rm "${ATLAS_TMP_TARBALL_PATH}"
  mv "apache-atlas-${ATLAS_VERSION}" "${ATLAS_HOME}"
}

function configure_solr(){
  if [[ $(hostname) == "${MASTER}" ]]; then
    # configure Solr only on the one actual Master node
    runuser -l solr -s /bin/bash -c "/usr/lib/solr/bin/solr create -c vertex_index -d /etc/atlas/conf/solr -shards 3"
    runuser -l solr -s /bin/bash -c "/usr/lib/solr/bin/solr create -c edge_index -d /etc/atlas/conf/solr -shards 3"
    runuser -l solr -s /bin/bash -c "/usr/lib/solr/bin/solr create -c fulltext_index -d /etc/atlas/conf/solr -shards 3"
  fi
}

function configure_atlas(){
  local zk_quorum=$(bdconfig get_property_value --configuration_file /etc/hbase/conf/hbase-site.xml \
    --name hbase.zookeeper.quorum 2>/dev/null)
  local cluster_name=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
  local zk_url_for_solr="$(echo ${zk_quorum} | sed "s/,/:2181\\\/solr,/g"):2181\\/solr"

  # symlink HBase conf dir
  mkdir /etc/atlas/hbase
  ln -s /etc/hbase/conf /etc/atlas/hbase/conf

  # configure atlas
  sed -i "s/atlas.graph.storage.hostname=.*/atlas.graph.storage.hostname=${zk_quorum}/" ${ATLAS_CONFIG}
  sed -i "s/atlas.graph.storage.hbase.table=.*/atlas.graph.storage.hbase.table=atlas/" ${ATLAS_CONFIG}

  sed -i "s/atlas.rest.address=.*/atlas.rest.address=http:\/\/$(hostname):21000/" ${ATLAS_CONFIG}
  sed -i "s/atlas.audit.hbase.zookeeper.quorum=.*/atlas.audit.hbase.zookeeper.quorum=${zk_quorum}/" ${ATLAS_CONFIG}

  if [[ -n "${ADDITIONAL_MASTER}" ]]; then
    # configure HA

    sed -i "s/atlas.server.ha.enabled=.*/atlas.server.ha.enabled=true/" ${ATLAS_CONFIG}
    sed -i "s/atlas.server.ha.zookeeper.connect=.*/atlas.server.ha.zookeeper.connect=${zk_quorum}/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.wait-searcher=.*/#atlas.graph.index.search.solr.wait-searcher=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-url=.*/atlas.graph.index.search.solr.zookeeper-url=${zk_url_for_solr}/" ${ATLAS_CONFIG}

    cat << EOF >> /etc/atlas/conf/atlas-application.properties
atlas.server.ids=m0,m1,m2
atlas.server.address.m0=${cluster_name}-m-0:21000
atlas.server.address.m1=${cluster_name}-m-1:21000
atlas.server.address.m2=${cluster_name}-m-2:21000
atlas.server.ha.zookeeper.zkroot=/apache_atlas
atlas.client.ha.retries=4
atlas.client.ha.sleep.interval.ms=5000
EOF

  else
    # disable solr cloud
    sed -i "s/atlas.graph.index.search.solr.mode=cloud/#atlas.graph.index.search.solr.mode=cloud/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-url=.*/#atlas.graph.index.search.solr.zookeeper-url=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-connect-timeout=.*/#atlas.graph.index.search.solr.zookeeper-connect-timeout=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.zookeeper-session-timeout=.*/#atlas.graph.index.search.solr.zookeeper-session-timeout=.*/" ${ATLAS_CONFIG}
    sed -i "s/atlas.graph.index.search.solr.wait-searcher=.*/#atlas.graph.index.search.solr.wait-searcher=.*/" ${ATLAS_CONFIG}

    # enable solr http
    sed -i "s/#atlas.graph.index.search.solr.mode=http/atlas.graph.index.search.solr.mode=http/" ${ATLAS_CONFIG}
    sed -i "s/#atlas.graph.index.search.solr.http-urls=.*/atlas.graph.index.search.solr.http-urls=http:\/\/${MASTER}:8983\/solr/" ${ATLAS_CONFIG}

  fi

  # override default admin username:password
  if [[ -n "${ATLAS_ADMIN_USERNAME}" && -n "${ATLAS_ADMIN_PASSWORD_SHA256}" ]]; then
    sed -i "s/admin=.*/${ATLAS_ADMIN_USERNAME}=ROLE_ADMIN::${ATLAS_ADMIN_PASSWORD_SHA256}/" "${ATLAS_HOME}/conf/users-credentials.properties"
  fi

  # configure for local Kafka
  if ls /usr/lib/kafka &>/dev/null; then
    sed -i "s/atlas.notification.embedded=.*/atlas.notification.embedded=false/" ${ATLAS_CONFIG}
    sed -i "s/atlas.kafka.zookeeper.connect=.*/atlas.kafka.zookeeper.connect=${zk_quorum}/" ${ATLAS_CONFIG}
    sed -i "s/atlas.kafka.bootstrap.servers=.*/atlas.kafka.bootstrap.servers=$(hostname):9092/" ${ATLAS_CONFIG}
  fi

}

function start_atlas(){
  cat << EOF > ${INIT_SCRIPT}
[Unit]
Description=Apache Atlas

[Service]
Type=forking
ExecStart=/etc/atlas/bin/atlas_start.py
ExecStop=/etc/atlas/bin/atlas_stop.py
RemainAfterExit=yes
TimeoutSec=10m

[Install]
WantedBy=multi-user.target
EOF
  chmod a+rw ${INIT_SCRIPT}
  systemctl enable atlas
  systemctl start atlas || err 'Unable to start atlas service'
}

function wait_for_atlas_to_start(){
  # atlas start script exits prematurely, before atlas actually starts
  # thus wait up to 10 minutes until atlas is fully working

  cmd='curl localhost:21000/api/atlas/admin/status'
  for ((i = 0; i < 60; i++)); do
    if eval "${cmd}"; then
      return 0
    fi
    sleep 10
  done
  return 1
}

function wait_for_atlas_becomes_active_or_passive(){
  cmd="sudo /etc/atlas/bin/atlas_admin.py -u doesnt:matter -status 2>/dev/null" # public check, but some username:password has to be given
  for ((i = 0; i < 60; i++)); do
    status=$(eval "${cmd}")
    if [[ ${status} == 'ACTIVE' || ${status} == 'PASSIVE' ]]; then
      return 0
    fi
    sleep 10
  done
  return 1
}

function enable_hive_hook(){
  bdconfig set_property \
    --name 'hive.exec.post.hooks' \
    --value 'org.apache.atlas.hive.hook.HiveHook' \
    --configuration_file '/etc/hive/conf/hive-site.xml' \
    --clobber

  echo "export HIVE_AUX_JARS_PATH=${ATLAS_HOME}/hook/hive" >> /etc/hive/conf/hive-env.sh
  ln -s ${ATLAS_CONFIG} /etc/hive/conf/
}

function enable_hbase_hook(){
  bdconfig set_property \
    --name 'hbase.coprocessor.master.classes' \
    --value 'org.apache.atlas.hbase.hook.HBaseAtlasCoprocessor' \
    --configuration_file '/etc/hbase/conf/hbase-site.xml' \
    --clobber

  ln -s /etc/atlas/hook/hbase/* /usr/lib/hbase/lib/
  ln -s ${ATLAS_CONFIG} /etc/hbase/conf/

  sudo service hbase-master restart
}

function enable_sqoop_hook(){
  if [[ ! -f  /usr/lib/sqoop ]]; then
    echo 'Sqoop not found, not configuring hook'
    return
  fi

  if [[ ! -f  /usr/lib/sqoop/conf/sqoop-site.xml ]]; then
    cp /usr/lib/sqoop/conf/sqoop-site-template.xml /usr/lib/sqoop/conf/sqoop-site.xml
  fi

  bdconfig set_property \
    --name 'sqoop.job.data.publish.class' \
    --value 'org.apache.atlas.sqoop.hook.SqoopHook' \
    --configuration_file '/usr/lib/sqoop/conf/sqoop-site.xml' \
    --clobber

  ln -s /etc/atlas/hook/sqoop/* /usr/lib/sqoop/lib
  ln -s ${ATLAS_CONFIG} /usr/lib/sqoop/conf

  sudo service hbase-master restart
}

function main() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

  if [[ "${role}" == 'Master' ]]; then
    check_prerequisites
    download_atlas
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
