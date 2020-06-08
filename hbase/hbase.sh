#!/bin/bash
#    Copyright 2018 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# This initialization action installs Apache HBase on Dataproc Cluster.

set -Eeuxo pipefail

readonly NOT_SUPPORTED_MESSAGE="HBase initialization action is not supported on Dataproc 2.0+.
Use HBase Component instead: https://cloud.google.com/dataproc/docs/concepts/components/hbase"
[[ $DATAPROC_VERSION = 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

readonly HBASE_HOME='/etc/hbase'
readonly CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
readonly WORKER_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly DATAPROC_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_ADDITIONAL=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional || true)
IFS=' ' read -r -a MASTER_HOSTNAMES <<<"${DATAPROC_MASTER} ${MASTER_ADDITIONAL//,/ }"
readonly ENABLE_KERBEROS=$(/usr/share/google/get_metadata_value attributes/enable-kerberos)
readonly KEYTAB_BUCKET=$(/usr/share/google/get_metadata_value attributes/keytab-bucket)
readonly DOMAIN=$(dnsdomainname)
readonly REALM=$(echo "${DOMAIN}" | awk '{print toupper($0)}')
readonly ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
readonly FQDN=$(hostname -f)

function retry_command() {
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
  retry_command "apt-get update"
}

function install_apt_get() {
  pkgs="$*"
  retry_command "apt-get install -y $pkgs"
}

function configure_hbase() {
  bdconfig set_property \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --name 'hbase.cluster.distributed' --value 'true' \
    --clobber

  bdconfig set_property \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --name 'hbase.zookeeper.property.initLimit' --value '20' \
    --clobber

  cat <<EOF >/etc/systemd/system/hbase-master.service
[Unit]
Description=HBase Master
Wants=network-online.target
After=network-online.target hadoop-hdfs-namenode.service

[Service]
User=root
Group=root
Type=simple
EnvironmentFile=/etc/environment
Environment=HBASE_HOME=/etc/hbase
ExecStart=/usr/bin/hbase \
  --config ${HBASE_HOME}/conf/ \
  master start

[Install]
WantedBy=multi-user.target
EOF

  cat <<EOF >/etc/systemd/system/hbase-regionserver.service
[Unit]
Description=HBase Regionserver
Wants=network-online.target
After=network-online.target hadoop-hdfs-datanode.service

[Service]
User=root
Group=root
Type=simple
EnvironmentFile=/etc/environment
Environment=HBASE_HOME=/etc/hbase
ExecStart=/usr/bin/hbase \
  --config ${HBASE_HOME}/conf/ \
  regionserver start

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload

  local hdfs_root="hdfs://${CLUSTER_NAME}"
  if [[ -z "${MASTER_ADDITIONAL}" ]]; then
    hdfs_root="hdfs://${CLUSTER_NAME}-m:8020"
  fi

  # Prepare and merge configuration values:
  # hbase.rootdir
  local hbase_root_dir
  hbase_root_dir="$(/usr/share/google/get_metadata_value attributes/hbase-root-dir ||
    echo "${hdfs_root}/hbase")"
  bdconfig set_property \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --name 'hbase.rootdir' --value "${hbase_root_dir}" \
    --clobber

  local hbase_wal_dir
  hbase_wal_dir=$(/usr/share/google/get_metadata_value attributes/hbase-wal-dir || true)
  if [[ -n ${hbase_wal_dir} ]]; then
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.wal.dir' --value "${hbase_wal_dir}" \
      --clobber
  fi

  # zookeeper.quorum
  local zookeeper_nodes
  zookeeper_nodes="$(grep '^server\.' /etc/zookeeper/conf/zoo.cfg |
    sort | uniq | cut -d '=' -f 2 | cut -d ':' -f 1 | xargs echo | sed "s/ /,/g")"
  bdconfig set_property \
    --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
    --name 'hbase.zookeeper.quorum' --value "${zookeeper_nodes}" \
    --clobber

  # Prepare kerberos specific config values for hbase-site.xml
  if [ "${ENABLE_KERBEROS}" = true ]; then

    # Kerberos authentication
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.security.authentication' --value "kerberos" \
      --clobber

    # Security authorization
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.security.authorization' --value "true" \
      --clobber

    # Kerberos master principal
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.master.kerberos.principal' --value "hbase/_HOST@${REALM}" \
      --clobber

    # Kerberos region server principal
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.regionserver.kerberos.principal' --value "hbase/_HOST@${REALM}" \
      --clobber

    # Kerberos master server keytab file path
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.master.keytab.file' --value "/etc/hbase/conf/hbase-master.keytab" \
      --clobber

    # Kerberos region server keytab file path
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.regionserver.keytab.file' --value "/etc/hbase/conf/hbase-region.keytab" \
      --clobber

    # Zookeeper authentication provider
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.zookeeper.property.authProvider.1' --value "org.apache.zookeeper.server.auth.SASLAuthenticationProvider" \
      --clobber

    # HBase coprocessor region classes
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.coprocessor.region.classes' --value "org.apache.hadoop.hbase.security.token.TokenProvider" \
      --clobber

    # Zookeeper remove host from principal
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.zookeeper.property.kerberos.removeHostFromPrincipal' --value "true" \
      --clobber

    # Zookeeper remove realm from principal
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.zookeeper.property.kerberos.removeRealmFromPrincipal' --value "true" \
      --clobber

    # Zookeeper znode
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'zookeeper.znode.parent' --value "/hbase-secure" \
      --clobber

    # HBase RPC protection
    bdconfig set_property \
      --configuration_file "${HBASE_HOME}/conf/hbase-site.xml" \
      --name 'hbase.rpc.protection' --value "privacy" \
      --clobber
  fi
  
  if [ "${ENABLE_KERBEROS}" = true ]; then
    if [[ "${HOSTNAME}" == "${DATAPROC_MASTER}" ]]; then
      # Master
      for m in "${MASTER_HOSTNAMES[@]}"; do
        kadmin.local -q "addprinc -randkey hbase/${m}.${DOMAIN}@${REALM}"
        echo "Generating hbase keytab..."
        kadmin.local -q "xst -k ${HBASE_HOME}/conf/hbase-${m}.keytab hbase/${m}.${DOMAIN}"
        gsutil cp "${HBASE_HOME}/conf/hbase-${m}.keytab" \
          "${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/hbase-${m}.keytab"
      done

      # Worker
      for ((c = 0; c < WORKER_COUNT; c++)); do
        kadmin.local -q "addprinc -randkey hbase/${CLUSTER_NAME}-w-${c}.${DOMAIN}"
        echo "Generating hbase keytab..."
        kadmin.local -q "xst -k ${HBASE_HOME}/conf/hbase-${CLUSTER_NAME}-w-${c}.keytab hbase/${CLUSTER_NAME}-w-${c}.${DOMAIN}"
        gsutil cp "${HBASE_HOME}/conf/hbase-${CLUSTER_NAME}-w-${c}.keytab" \
          "${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/hbase-${CLUSTER_NAME}-w-${c}.keytab"
      done
      touch /tmp/_success
      gsutil cp /tmp/_success "${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/_success"
    fi
    success=1
    while [[ $success == "1" ]]; do
      sleep 1
      success=$(
        gsutil -q stat "${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/_success"
        echo $?
      )
    done

    # Define keytab path based on role
    if [[ "${ROLE}" == 'Master' ]]; then
      hbase_keytab_path=${HBASE_HOME}/conf/hbase-master.keytab
    else
      hbase_keytab_path=${HBASE_HOME}/conf/hbase-region.keytab
    fi

    # Copy keytab to machine
    gsutil cp "${KEYTAB_BUCKET}/keytabs/${CLUSTER_NAME}/hbase-${HOSTNAME}.keytab" $hbase_keytab_path

    # Change owner of keytab to hbase with read only permissions
    if [ -f $hbase_keytab_path ]; then
      chown hbase:hbase $hbase_keytab_path
      chmod 0400 $hbase_keytab_path
    fi

    # Change regionserver information
    for ((c = 0; c < WORKER_COUNT; c++)); do
      echo "${CLUSTER_NAME}-w-${c}.${DOMAIN}" >>/tmp/regionservers
    done
    mv /tmp/regionservers ${HBASE_HOME}/conf/regionservers

    # Add server JAAS
    cat >/tmp/hbase-server.jaas <<EOF
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  storeKey=true
  useTicketCache=false
  keyTab="${hbase_keytab_path}"
  principal="hbase/${FQDN}";
};
EOF

    # Copy JAAS file to hbase conf directory
    mv /tmp/hbase-server.jaas ${HBASE_HOME}/conf/hbase-server.jaas

    # Add client JAAS
    cat >/tmp/hbase-client.jaas <<EOF
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=false
  useTicketCache=true;
};
EOF

    # Copy JAAS file to hbase conf directory
    mv /tmp/hbase-client.jaas ${HBASE_HOME}/conf/hbase-client.jaas

    # Extend hbase enviroment variable script
    cat ${HBASE_HOME}/conf/hbase-env.sh >/tmp/hbase-env.sh
    cat >>/tmp/hbase-env.sh <<EOF
export HBASE_MANAGES_ZK=false
export HBASE_OPTS="\$HBASE_OPTS -Djava.security.auth.login.config=/etc/hbase/conf/hbase-client.jaas"
export HBASE_MASTER_OPTS="\$HBASE_MASTER_OPTS -Djava.security.auth.login.config=/etc/hbase/conf/hbase-server.jaas"
export HBASE_REGIONSERVER_OPTS="\$HBASE_REGIONSERVER_OPTS -Djava.security.auth.login.config=/etc/hbase/conf/hbase-server.jaas"
EOF

    # Copy script to hbase conf directory
    mv /tmp/hbase-env.sh ${HBASE_HOME}/conf/hbase-env.sh
  fi

  # On single node clusters we must also start regionserver on it.
  if [[ "${WORKER_COUNT}" -eq 0 ]]; then
    systemctl start hbase-regionserver
  fi
}

function main() {
  update_apt_get
  install_apt_get hbase

  configure_hbase

  if [[ "${ROLE}" == 'Master' ]]; then
    systemctl start hbase-master
  else
    systemctl start hbase-regionserver
  fi
}

main
