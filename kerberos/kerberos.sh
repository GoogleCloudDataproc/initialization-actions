#!/bin/bash
#    Copyright 2015 Google, Inc.
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
# This script installs and configures Kerberos on a Google Cloud
# Dataproc cluster.

# Do not use -x to avoid printing passwords.
set -euo pipefail

# Fail under HA mode early
additional_nodes=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional | sed 's/,/\n/g' | wc -l)
if [[ $additional_nodes -gt 0 ]]; then
  echo 'Kerberos is not supported on HA mode clusters.'
  exit 1
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

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
WORKER_COUNT="$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)"
FQDN="$(hostname -f | tr -d '\n')"
DOMAIN=${FQDN#*\.}
REALM=$(echo $DOMAIN | awk '{print toupper($0)}')
MASTER="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
MASTER_FQDN="$MASTER.$DOMAIN"
HADOOP_CONF_DIR="/etc/hadoop/conf"

readonly kms_key_uri=$(/usr/share/google/get_metadata_value attributes/kms-key-uri)
readonly db_password_uri=$(/usr/share/google/get_metadata_value attributes/db-password-uri)
readonly root_password_uri=$(/usr/share/google/get_metadata_value attributes/root-password-uri)
readonly keystore_password_uri=$(/usr/share/google/get_metadata_value attributes/keystore-password-uri)
readonly db_password=$(gsutil cat $db_password_uri | \
  gcloud kms decrypt \
  --ciphertext-file - \
  --plaintext-file - \
  --key $kms_key_uri)
readonly root_principal_password=$(gsutil cat $root_password_uri | \
  gcloud kms decrypt \
  --ciphertext-file - \
  --plaintext-file - \
  --key $kms_key_uri)
readonly keystore_password=$(gsutil cat $keystore_password_uri | \
  gcloud kms decrypt \
  --ciphertext-file - \
  --plaintext-file - \
  --key $kms_key_uri)
readonly dataproc_bucket=$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)
readonly keystore_bucket=$(/usr/share/google/get_metadata_value attributes/keystore-bucket)
readonly cluster_uuid=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-uuid)
readonly krb5_server_mark_file="krb5-server-mark"

function install_and_start_kerberos_server() {
  # Install the krb5-kdc and krb5-admin-server packages
  DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-kdc; \
  DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-admin-server || TRUE
  krb5_configuration

  # Retrieve the password to new db and generate new db
  # Worth mentioning is that the '-W' option will force use /dev/urandom instead
  # of /dev/random for entropy, which will help speed up db creation, but people
  # argue that urandom does not provide guarantee of randomness.
  echo -e "$db_password\n$db_password" | /usr/sbin/kdb5_util create -s -W

  service krb5-kdc restart || err 'Cannot restart KDC'

  # Give principal 'root' admin access
  cat << EOF >> /etc/krb5kdc/kadm5.acl
root *
EOF

  service krb5-admin-server restart || err 'Cannot restart Kerberos admin server'
}

function install_kerberos_client() {
  DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-user; \
  DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-config || TRUE
  krb5_configuration
}

function install_kerberos() {
  if [[ $ROLE == 'Master' ]]; then
    install_and_start_kerberos_server
  else
    install_kerberos_client
  fi
}

function krb5_configuration() {
  sed -i "/\[realms\]/a\ \t$REALM = {\n\t\tkdc = $MASTER_FQDN\n\t\tadmin_server = $MASTER_FQDN\n\t}" "/etc/krb5.conf"
}

function restart_kdc() {
  if [[ $ROLE == 'Master' ]]; then
    service krb5-kdc restart || err 'Cannot restart KDC'
    service krb5-admin-server restart || err 'Cannot restart Kerberos admin server'
  fi
}

function create_root_user_principal() {
  if [[ $ROLE == 'Master' ]]; then
    echo -e "$root_principal_password\n$root_principal_password" | /usr/sbin/kadmin.local -q "addprinc root"
    # write a file to gcs to signal to workers Kerberos server is ready (root
    # principal is also created)
    touch /tmp/$krb5_server_mark_file
    gsutil cp /tmp/$krb5_server_mark_file gs://$dataproc_bucket/$cluster_uuid/$krb5_server_mark_file
    rm /tmp/$krb5_server_mark_file
  fi
}

function install_jce() {
  cp $JAVA_HOME/jre/lib/security/policy/unlimited/*.jar $JAVA_HOME/jre/lib/security
}

function create_service_principals() {
  if [[ $ROLE == 'Worker' ]]; then
    local server_started=1
    for (( i=0; i < 5*60; i++ )); do
      echo "wait for the $i iteration"
      if gsutil -q stat gs://$dataproc_bucket/$cluster_uuid/$krb5_server_mark_file; then
        server_started=0
        break
      fi
      sleep 1
    done

    if [[ $server_started == 1 ]]; then
      echo 'Kerberos server has not started even after 5 minutes, likely failed'
      exit 1
    fi
  fi

  echo "starting to create service principals on $FQDN"
  # principals: hdfs/<FQDN>, yarn/<FQDN>, mapred/<FQDN> and HTTP/<FQDN>
  kadmin -p root -w $root_principal_password -q "addprinc -randkey hdfs/$FQDN"
  kadmin -p root -w $root_principal_password -q "addprinc -randkey yarn/$FQDN"
  kadmin -p root -w $root_principal_password -q "addprinc -randkey mapred/$FQDN"
  kadmin -p root -w $root_principal_password -q "addprinc -randkey HTTP/$FQDN"

  # keytabs
  kadmin -p root -w $root_principal_password -q "xst -k hdfs.keytab hdfs/$FQDN HTTP/$FQDN"
  kadmin -p root -w $root_principal_password -q "xst -k yarn.keytab yarn/$FQDN HTTP/$FQDN"
  kadmin -p root -w $root_principal_password -q "xst -k mapred.keytab mapred/$FQDN HTTP/$FQDN"
  kadmin -p root -w $root_principal_password -q "xst -k http.keytab HTTP/$FQDN"

  # Move keytabs to /etc/hadoop/conf and set permissions on them
  mv *.keytab $HADOOP_CONF_DIR
  chown hdfs:hadoop $HADOOP_CONF_DIR/hdfs.keytab
  chown yarn:hadoop $HADOOP_CONF_DIR/yarn.keytab
  chown mapred:hadoop $HADOOP_CONF_DIR/mapred.keytab
  chown hdfs:hadoop $HADOOP_CONF_DIR/http.keytab
  chmod 400 $HADOOP_CONF_DIR/*.keytab
}

function set_property_in_xml() {
  bdconfig set_property \
    --configuration_file $1 \
    --name $2 --value $3 \
    --create_if_absent \
    --clobber \
    || err "Unable to set $2"
}

function set_property_core_site() {
  set_property_in_xml "$HADOOP_CONF_DIR/core-site.xml" $1 $2
}

function config_core_site() {
  set_property_core_site 'hadoop.security.authentication' 'kerberos'
  set_property_core_site 'hadoop.security.authorization' 'true'
  set_property_core_site 'hadoop.rpc.protection' 'privacy'
  set_property_core_site 'hadoop.ssl.enabled' 'true'
  set_property_core_site 'hadoop.ssl.require.client.cert' 'false'
  set_property_core_site 'hadoop.ssl.hostname.verifier' 'DEFAULT'
  set_property_core_site 'hadoop.ssl.keystores.factory.class' 'org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory'
  set_property_core_site 'hadoop.ssl.server.conf' 'ssl-server.xml'
  set_property_core_site 'hadoop.ssl.client.conf' 'ssl-client.xml'
  set_property_core_site 'fs.default.name' "hdfs://$MASTER_FQDN"
  set_property_core_site 'fs.defaultFS' "hdfs://$MASTER_FQDN"
}

function set_property_hdfs_site() {
  set_property_in_xml "$HADOOP_CONF_DIR/hdfs-site.xml" $1 $2
}

function config_hdfs_site() {
  set_property_hdfs_site 'dfs.block.access.token.enable' 'true'
  set_property_hdfs_site 'dfs.encrypt.data.transfer' 'true'
  # Name Node
  set_property_hdfs_site 'dfs.namenode.keytab.file' "$HADOOP_CONF_DIR/hdfs.keytab"
  set_property_hdfs_site 'dfs.namenode.kerberos.principal' "hdfs/_HOST@$REALM"
  set_property_hdfs_site 'dfs.namenode.kerberos.internal.spnego.principal' "HTTP/_HOST@$REALM"
  # Secondary Name Node
  set_property_hdfs_site 'dfs.secondary.namenode.keytab.file' "$HADOOP_CONF_DIR/hdfs.keytab"
  set_property_hdfs_site 'dfs.secondary.namenode.kerberos.principal' "hdfs/_HOST@$REALM"
  set_property_hdfs_site 'dfs.secondary.namenode.kerberos.internal.spnego.principal' "HTTP/_HOST@$REALM"
  # Journal Node
  set_property_hdfs_site 'dfs.journalnode.keytab.file' "$HADOOP_CONF_DIR/hdfs.keytab"
  set_property_hdfs_site 'dfs.journalnode.kerberos.principal' "hdfs/_HOST@$REALM"
  set_property_hdfs_site 'dfs.journalnode.kerberos.internal.spnego.principal' "HTTP/_HOST@$REALM"
  # Data Node
  # 'dfs.data.transfer.protection' Enable SASL, only supported after version 2.6
  set_property_hdfs_site 'dfs.data.transfer.protection' 'privacy'
  unset HADOOP_SECURE_DN_USER  # Required by SASL
  set_property_hdfs_site 'dfs.datanode.keytab.file' "$HADOOP_CONF_DIR/hdfs.keytab"
  set_property_hdfs_site 'dfs.datanode.kerberos.principal' "hdfs/_HOST@$REALM"
  # WebHDFS
  set_property_hdfs_site 'dfs.webhdfs.enabled' 'true'
  set_property_hdfs_site 'dfs.web.authentication.kerberos.principal' "HTTP/_HOST@$REALM"
  set_property_hdfs_site 'dfs.web.authentication.kerberos.keytab' "$HADOOP_CONF_DIR/http.keytab"
  # HTTPS
  set_property_hdfs_site 'dfs.http.policy' 'HTTPS_ONLY'

  set_property_hdfs_site 'dfs.namenode.rpc-address' "$MASTER_FQDN:8020"
}

function set_property_yarn_site() {
  set_property_in_xml $HADOOP_CONF_DIR/yarn-site.xml $1 $2
}

function config_yarn_site_and_permission() {
  # Resource Manager
  set_property_yarn_site 'yarn.resourcemanager.keytab' "$HADOOP_CONF_DIR/yarn.keytab"
  set_property_yarn_site 'yarn.resourcemanager.principal' "yarn/_HOST@$REALM"
  set_property_yarn_site 'yarn.resourcemanager.hostname' "$MASTER_FQDN"
  # Node Manager
  set_property_yarn_site 'yarn.nodemanager.keytab' "$HADOOP_CONF_DIR/yarn.keytab"
  set_property_yarn_site 'yarn.nodemanager.principal' "yarn/_HOST@$REALM"
  set_property_yarn_site 'yarn.nodemanager.container-executor.class' 'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor'
  set_property_yarn_site 'yarn.nodemanager.linux-container-executor.group' 'yarn'

  # HTTPS
  set_property_yarn_site 'yarn.http.policy' 'HTTPS_ONLY'
  set_property_yarn_site 'yarn.log.server.url' "https://$MASTER_FQDN:19889/jobhistory/logs"

  # TODO: did not find a way to allow root. Seems to be a big problem
  # if we consier submitting jobs through agent, which runs as root.
  cat << EOF > $HADOOP_CONF_DIR/container-executor.cfg
yarn.nodemanager.linux-container-executor.group=yarn
banned.users=hdfs,yarn,mapred,bin
min.user.id=1000
EOF
}

function set_property_mapred_site() {
  set_property_in_xml $HADOOP_CONF_DIR/mapred-site.xml $1 $2
}

function config_mapred_site() {
  if [[ $ROLE == 'Master' ]]; then
    set_property_mapred_site 'mapreduce.jobhistory.keytab' "$HADOOP_CONF_DIR/mapred.keytab"
    set_property_mapred_site 'mapreduce.jobhistory.principal' "mapred/_HOST@$REALM"
  fi

  set_property_mapred_site 'mapreduce.jobhistory.http.policy' 'HTTPS_ONLY'
  set_property_mapred_site 'mapreduce.ssl.enabled' 'true'
  set_property_mapred_site 'mapreduce.shuffle.ssl.enabled' 'true'
  set_property_mapred_site 'mapreduce.jobhistory.address' "$MASTER_FQDN:10020"
  set_property_mapred_site 'mapreduce.jobhistory.webapp.https.address' "$MASTER_FQDN:19889"
}

function copy_keystore_files() {
  mkdir $HADOOP_CONF_DIR/ssl
  gsutil cp gs://$keystore_bucket/keystore.jks $HADOOP_CONF_DIR/ssl
  gsutil cp gs://$keystore_bucket/truststore.jks $HADOOP_CONF_DIR/ssl
  chown -R yarn:hadoop $HADOOP_CONF_DIR/ssl
  chmod 755 $HADOOP_CONF_DIR/ssl
  chmod 440 $HADOOP_CONF_DIR/ssl/keystore.jks
  chmod 444 $HADOOP_CONF_DIR/ssl/truststore.jks
}

function set_property_ssl_xml() {
  set_property_in_xml $HADOOP_CONF_DIR/ssl-$1.xml $2 $3
}

function config_ssl_xml() {
  cp $HADOOP_CONF_DIR/ssl-$1.xml.example $HADOOP_CONF_DIR/ssl-$1.xml
  set_property_ssl_xml $1 "ssl.$1.truststore.location" "$HADOOP_CONF_DIR/ssl/truststore.jks"
  set_property_ssl_xml $1 "ssl.$1.truststore.password" "$keystore_password"
  set_property_ssl_xml $1 "ssl.$1.truststore.type" "jks"
  set_property_ssl_xml $1 "ssl.$1.keystore.location" "$HADOOP_CONF_DIR/ssl/keystore.jks"
  set_property_ssl_xml $1 "ssl.$1.keystore.password" "$keystore_password"
  set_property_ssl_xml $1 "ssl.$1.keystore.keypassword" "$keystore_password"
  set_property_ssl_xml $1 "ssl.$1.keystore.type" "jks"
}

function set_property_hive_site() {
  set_property_in_xml "/etc/hive/conf/hive-site.xml" $1 $2
}

function secure_hive() {
  if [[ $ROLE == 'Master' ]]; then
    kadmin -p root -w $root_principal_password -q "addprinc -randkey hive/$FQDN"
    kadmin -p root -w $root_principal_password -q "xst -k hive.keytab hive/$FQDN"
    mv ./hive.keytab /etc/hive/conf/
    chown hive:hive /etc/hive/conf/hive.keytab
    chmod 400 /etc/hive/conf/hive.keytab
    # Add the user 'hive' to group 'hadoop' so it can access the keystore files
    usermod -a -G hadoop hive
    # HiveServer2
    set_property_hive_site 'hive.server2.authentication' 'KERBEROS'
    set_property_hive_site 'hive.server2.authentication.kerberos.principal' "hive/_HOST@$REALM"
    set_property_hive_site 'hive.server2.authentication.kerberos.keytab' '/etc/hive/conf/hive.keytab'
    set_property_hive_site 'hive.server2.thrift.sasl.qop' 'auth-conf'
    # Hive Metastore
    set_property_hive_site 'hive.metastore.sasl.enabled' 'true'
    set_property_hive_site 'hive.metastore.kerberos.principal' "hive/_HOST@$REALM"
    set_property_hive_site 'hive.metastore.kerberos.keytab.file' '/etc/hive/conf/hive.keytab'
    # WebUI SSL
    set_property_hive_site 'hive.server2.webui.use.ssl' 'true'
    set_property_hive_site 'hive.server2.webui.keystore.path' "$HADOOP_CONF_DIR/ssl/keystore.jks"
    set_property_hive_site 'hive.server2.webui.keystore.password' $keystore_password
  fi
}

function secure_spark() {
  if [[ $ROLE == 'Master' ]]; then
    kadmin -p root -w $root_principal_password -q "addprinc -randkey spark/$FQDN"
    kadmin -p root -w $root_principal_password -q "xst -k spark.keytab spark/$FQDN"
    mv ./spark.keytab /etc/spark/conf/
    chown spark:spark /etc/spark/conf/spark.keytab
    chmod 400 /etc/spark/conf/spark.keytab
    # Add the user 'spark' to group 'hadoop' so it can access the keystore files
    usermod -a -G hadoop spark
    cat << EOF >> /etc/spark/conf/spark-env.sh
SPARK_HISTORY_OPTS="-Dspark.history.kerberos.enabled=true
-Dspark.history.kerberos.principal=spark/$FQDN@$REALM
-Dspark.history.kerberos.keytab=/etc/spark/conf/spark.keytab"
EOF
    cat << EOF >> /etc/spark/conf/spark-defaults.conf
spark.yarn.historyServer.address https://$FQDN:18480
spark.ssl.protocol tls
spark.ssl.historyServer.enabled true
spark.ssl.trustStore $HADOOP_CONF_DIR/ssl/truststore.jks
spark.ssl.keyStore $HADOOP_CONF_DIR/ssl/keystore.jks
spark.ssl.trustStorePassword $keystore_password
spark.ssl.keyStorePassword $keystore_password
EOF
  fi
}

function mark_kerberized() {
  touch /etc/google-dataproc/kerberized
}

function restart_services() {
  if [[ $ROLE == 'Master' ]]; then
    service hadoop-hdfs-namenode restart || err 'Cannot restart name node'
    service hadoop-hdfs-secondarynamenode restart || err 'Cannot restart secondary name node'
    service hadoop-mapreduce-historyserver restart || err 'Cannot restart mapreduce history server'
    service hadoop-yarn-resourcemanager restart || err 'Cannot restart yarn resource manager'
    service hive-server2 restart || err 'Cannot restart hive server'
    service hive-metastore restart || err 'Cannot restart hive metastore'
    service spark-history-server restart || err 'Cannot restart spark history server'
  fi
  # In single node mode, we have to run datanode and nodemanager on the master.
  if [[ $ROLE == 'Worker' || $WORKER_COUNT == '0' ]]; then
    service hadoop-hdfs-datanode restart || err 'Cannot restart data node'
    service hadoop-yarn-nodemanager restart || err 'Cannot restart node manager'
  fi
}

function main() {
  update_apt_get
  install_kerberos
  install_jce
  create_root_user_principal
  create_service_principals
  config_core_site
  config_hdfs_site
  config_yarn_site_and_permission
  config_mapred_site
  copy_keystore_files
  config_ssl_xml 'server'
  config_ssl_xml 'client'
  secure_hive
  secure_spark
  restart_services
  mark_kerberized
}

main
