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
# Dataproc cluster. It also enables SSL encryption (using user provided
# certificate) for shuffle and Web UIs.

# Do not use -x, so that passwords are not printed.
set -euo pipefail

# Whether init actions will run early
# This will affect whether nodemanager and datanode should be restarted
readonly early_init="$(/usr/share/google/get_metadata_value attributes/dataproc-option-run-init-actions-early || echo 'false')"

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

CLUSTER_UUID="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-uuid)"
CLUSTER_NAME="$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)"
ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
WORKER_COUNT="$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)"
ADDITIONAL_MASTERS=($(/usr/share/google/get_metadata_value attributes/dataproc-master-additional | sed 's/,/\n/g'))
HA_MODE=$((${#ADDITIONAL_MASTERS[@]}>0))
if [[ "${HA_MODE}" == 1 ]]; then
  MASTERS=("${CLUSTER_NAME}-m-0" "${CLUSTER_NAME}-m-1" "${CLUSTER_NAME}-m-2")
  KRB5_PROP_SCRIPT="/etc/krb5_prop.sh"
fi
FQDN="$(hostname -f | tr -d '\n')"
DOMAIN="${FQDN#*\.}"
REALM="$(echo ${DOMAIN} | awk '{print toupper($0)}')"
# In HA mode, this will be the 'first-master', i.e. $CLUSTER_NAME-m-0
MASTER="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
MASTER_FQDN="${MASTER}.${DOMAIN}"
HADOOP_CONF_DIR="/etc/hadoop/conf"

readonly kms_key_uri="$(/usr/share/google/get_metadata_value attributes/kms-key-uri)"
readonly root_password_uri="$(/usr/share/google/get_metadata_value attributes/root-password-uri)"

# If user does not specify the URI of the encrypted file containing the KDC
# database password (master key) the the password defaults to "dataproc".
readonly db_password_uri="$(/usr/share/google/get_metadata_value attributes/db-password-uri)" || echo ''
db_password="dataproc"
if [[ -n "${db_password_uri}" ]]; then
  db_password="$(gsutil cat ${db_password_uri} | \
    gcloud kms decrypt \
    --ciphertext-file - \
    --plaintext-file - \
    --key ${kms_key_uri})"
fi

readonly root_principal_password="$(gsutil cat ${root_password_uri} | \
  gcloud kms decrypt \
  --ciphertext-file - \
  --plaintext-file - \
  --key ${kms_key_uri})"

# SSL keystore / truststore. By default a self signed certificate is used, but
# user can specify the location of their own certificate through the following
# metadata key-value pairs. If user specifies the keystore URI they must also
# specify the truststore URI and the keystore password.
readonly keystore_uri="$(/usr/share/google/get_metadata_value attributes/keystore-uri)" || echo ''
if [[ -n "${keystore_uri}" ]]; then
  readonly keystore_password_uri="$(/usr/share/google/get_metadata_value attributes/keystore-password-uri)" || echo ''
  if [[ -n "${keystore_password_uri}" ]]; then
    error "Missing parameter 'keystore_password_uri'
  fi
  readonly keystore_password="$(gsutil cat ${keystore_password_uri} | \
    gcloud kms decrypt \
    --ciphertext-file - \
    --plaintext-file - \
    --key ${kms_key_uri})"
  readonly truststore_uri="$(/usr/share/google/get_metadata_value attributes/truststore-uri)" || echo '';
  if [[ -n "${truststore_uri}" ]]; then
    error "Missing parameter 'truststore_uri'
  fi
else
  readonly keystore_password="dataproc"
fi

# Cross-realm trust
readonly cross_realm_trust_realm="$(/usr/share/google/get_metadata_value attributes/cross-realm-trust-realm)" || echo ''
if [[ -n "${cross_realm_trust_realm}" ]]; then
  readonly cross_realm_trust_kdc="$(/usr/share/google/get_metadata_value attributes/cross-realm-trust-kdc)"
  readonly cross_realm_trust_admin_server="$(/usr/share/google/get_metadata_value attributes/cross-realm-trust-admin-server)"
  readonly cross_realm_trust_password_uri="$(/usr/share/google/get_metadata_value attributes/cross-realm-trust-password-uri)"
  readonly trust_password="$(gsutil cat ${cross_realm_trust_password_uri} | \
    gcloud kms decrypt \
    --ciphertext-file - \
    --plaintext-file - \
    --key ${kms_key_uri})"
fi

# GCS locations, etc. for staging/node coordinations.
readonly dataproc_bucket="$(/usr/share/google/get_metadata_value attributes/dataproc-bucket)"
readonly staging_meta_info_gcs_prefix="${dataproc_bucket}/google-cloud-dataproc-metainfo/${CLUSTER_UUID}/secure_init_action"
readonly krb5_server_mark_file="krb5-server-mark"

function install_and_start_kerberos_server() {
  echo "Installing krb5-kdc and krb5-admin-server."
  DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-kdc krb5-admin-server
  krb5_configuration

  # Retrieve the password to new db and generate new db
  # The '-W' option will force the use of /dev/urandom instead
  # of /dev/random for entropy, which will help speed up db creation.
  # Only run this from the Master-KDC in HA mode.
  if [[ "${FQDN}" == "${MASTER_FQDN}" ]]; then
    echo "Creating KDC database."
    echo -e "${db_password}\n${db_password}" | /usr/sbin/kdb5_util create -s -W
  fi

  # Give principal 'root' admin access
  cat << EOF >> /etc/krb5kdc/kadm5.acl
root *
EOF

  if [[ "${FQDN}" == "${MASTER_FQDN}" ]]; then
    echo "Restarting krb5-kdc on Master KDC."
    systemctl restart krb5-kdc || err 'Cannot restart KDC'

    echo "Restarting krb5-admin-server on Master KDC."
    systemctl restart krb5-admin-server || err 'Cannot restart Kerberos admin server'
  fi
}

function install_kerberos_client() {
  echo "Installing krb5-user and krb5-config."
  DEBIAN_FRONTEND=noninteractive apt-get install -y krb5-user krb5-config
  krb5_configuration
}

function install_kerberos() {
  if [[ "${ROLE}" == 'Master' ]]; then
    install_and_start_kerberos_server
  else
    install_kerberos_client
  fi
}

function krb5_configuration() {
  if [[ -n "${cross_realm_trust_realm}" ]]; then
    echo "Configruing cross-realm trust to ${cross_realm_trust_realm}"
    sed -i "/\[realms\]/a\ \t${cross_realm_trust_realm} = {\n\t\tkdc = ${cross_realm_trust_kdc}\n\t\tadmin_server = ${cross_realm_trust_admin_server}\n\t}" "/etc/krb5.conf"
    sed -i "/\[domain_realm\]/a\ \t${cross_realm_trust_kdc} = ${cross_realm_trust_realm}\n\t\.${DOMAIN} = ${REALM}" "/etc/krb5.conf"
  fi
  echo "Configuring krb5 to add local realm in /etc/krb5.conf."
  additional_kdcs=""
  if [[ "${HA_MODE}" == 1 ]]; then
    for additional_master in "${ADDITIONAL_MASTERS[@]}"; do
      additional_kdcs+="\t\tkdc = ${additional_master}.${DOMAIN}\n"
    done
  fi
  sed -i "/\[realms\]/a\ \t${REALM} = {\n\t\tkdc = ${MASTER_FQDN}\n${additional_kdcs}\t\tadmin_server = ${MASTER_FQDN}\n\t}" "/etc/krb5.conf"
}

function create_root_user_principal_and_ready_stash() {
  if [[ "${FQDN}" == "${MASTER_FQDN}" ]]; then
    echo "Creating root principal root@${REALM}."
    echo -e "${root_principal_password}\n${root_principal_password}" | /usr/sbin/kadmin.local -q "addprinc root"
    if [[ "${HA_MODE}" == 1 ]]; then
      echo "In HA mode, writing krb5 stash file to GCS: gs://${staging_meta_info_gcs_prefix}/stash"
      gsutil cp /etc/krb5kdc/stash "gs://${staging_meta_info_gcs_prefix}/stash"
    fi
    # write a file to gcs to signal to workers (and possibly additional masters)
    # that Kerberos server on Master KDC is ready
    # (root principal is also created).
    echo "Writing krb5 server mark file in GCS: gs://${staging_meta_info_gcs_prefix}/${krb5_server_mark_file}"
    touch /tmp/"${krb5_server_mark_file}"
    gsutil cp /tmp/"${krb5_server_mark_file}" "gs://${staging_meta_info_gcs_prefix}/${krb5_server_mark_file}"
    rm /tmp/"${krb5_server_mark_file}"
  fi
}

function create_cross_realm_trust_principal_if_necessary() {
  if [[ "${FQDN}" == "${MASTER_FQDN}" && -n "${cross_realm_trust_realm}" ]]; then
    echo "Creating cross-realm trust principal krbtgt/${REALM}@${cross_realm_trust_realm}"
    echo -e "${trust_password}\n${trust_password}" | /usr/sbin/kadmin.local -q "addprinc krbtgt/${REALM}@${cross_realm_trust_realm}"
  fi
}

function install_jce() {
  cp "${JAVA_HOME}"/jre/lib/security/policy/unlimited/*.jar "${JAVA_HOME}"/jre/lib/security
}

function create_service_principals() {
  if [[ "${FQDN}" != "${MASTER_FQDN}" ]]; then
    local server_started=0
    for (( i=0; i < 5*60; i++ )); do
      echo "Waiting for KDC and root principal."
      if gsutil -q stat "gs://${staging_meta_info_gcs_prefix}/${krb5_server_mark_file}"; then
        server_started=1
        break
      fi
      sleep 1
    done

    if [[ "${server_started}" == 0 ]]; then
      echo 'Kerberos server has not started even after 5 minutes, likely failed.'
      exit 1
    fi
  fi

  if [[ "${HA_MODE}" == 1 && "${ROLE}" == 'Master' ]]; then
    echo "Creating host principal on ${FQDN}"
    kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey host/${FQDN}"
    kadmin -p root -w "${root_principal_password}" -q "ktadd host/${FQDN}"
    if [[ "${FQDN}" != "${MASTER_FQDN}" ]]; then
      gsutil cp "gs://${staging_meta_info_gcs_prefix}/stash" /etc/krb5kdc/stash
    fi
  fi

  echo "Creating service principals on ${FQDN}"
  # principals: hdfs/<FQDN>, yarn/<FQDN>, mapred/<FQDN> and HTTP/<FQDN>
  kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey hdfs/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey yarn/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey mapred/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey HTTP/${FQDN}"

  # keytabs
  kadmin -p root -w "${root_principal_password}" -q "xst -k hdfs.keytab hdfs/${FQDN} HTTP/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "xst -k yarn.keytab yarn/${FQDN} HTTP/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "xst -k mapred.keytab mapred/${FQDN} HTTP/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "xst -k http.keytab HTTP/${FQDN}"

  # Move keytabs to /etc/hadoop/conf and set permissions on them
  mv *.keytab "${HADOOP_CONF_DIR}"
  chown hdfs:hadoop "${HADOOP_CONF_DIR}"/hdfs.keytab
  chown yarn:hadoop "${HADOOP_CONF_DIR}"/yarn.keytab
  chown mapred:hadoop "${HADOOP_CONF_DIR}"/mapred.keytab
  chown hdfs:hadoop "${HADOOP_CONF_DIR}"/http.keytab
  chmod 400 "${HADOOP_CONF_DIR}"/*.keytab
  # Allow group 'hadoop' to read the http.keytab file.
  chmod 440 "${HADOOP_CONF_DIR}"/http.keytab
}

function config_krb5_propagate_if_necessary() {
  if [[ "${HA_MODE}" == 1 && "${ROLE}" == 'Master' ]]; then
    cat << EOF >> /etc/services
krb5_prop 754/tcp
EOF
    echo "Installing xinetd"
    apt-get install -y xinetd
    cat << EOF > /etc/xinetd.d/krb5_prop
service krb5_prop
{
        disable         = no
        socket_type     = stream
        protocol        = tcp
        user            = root
        wait            = no
        server          = /usr/sbin/kpropd
}
EOF
    systemctl restart xinetd

    # Assuming there will be 3 masters which are $CLUSTER_NAME-m-0,
    # $CLUSTER_NAME-m-1 and $CLUSTER_NAME-m-2
    # To include all 3 masters in the ACL files on all 3 masters makes
    # failover easier if it is needed.
    cat << EOF >> /etc/krb5kdc/kpropd.acl
host/$CLUSTER_NAME-m-0.$DOMAIN@$REALM
host/$CLUSTER_NAME-m-1.$DOMAIN@$REALM
host/$CLUSTER_NAME-m-2.$DOMAIN@$REALM
EOF

    if [[ "${FQDN}" != "${MASTER_FQDN}" ]]; then
      touch "krb5_prop_acl_mark_$FQDN"
      gsutil cp "./krb5_prop_acl_mark_$FQDN" "gs://${staging_meta_info_gcs_prefix}/krb5_prop_acl/krb5_prop_acl_mark_$FQDN"
      rm "krb5_prop_acl_mark_$FQDN"
      local krb5_prop_finish=0
      for (( i=0; i < 5*60; i++)); do
        echo "Waiting for initial KDC database propagation to finish..."
        if gsutil -q stat "gs://${staging_meta_info_gcs_prefix}/${FQDN}-propagated"; then
          krb5_prop_finish=1
          break
        fi
        sleep 1
      done

      if [[ "${krb5_prop_finish}" == 0 ]]; then
        echo 'Initial KDC database propagation did not finish after 5 minutes, likely failed.'
        exit 1
      fi

      echo "Restarting krb5-kdc on ${FQDN}."
      systemctl restart krb5-kdc || err 'Cannot restart KDC'
      echo "Restarting krb5-admin-server on ${FQDN}."
      systemctl restart krb5-admin-server || err 'Cannot restart Kerberos admin server'
    else
      # On Master-KDC
      /usr/sbin/kdb5_util dump /etc/krb5kdc/slave_datatrans
      local krb5_prop_acl_configured=0
      for (( i=0; i < 5*60; i++ )); do
        echo "Waiting for all slave KDCs to finish krb_prop_acl configuration."
        finished_count="$((gsutil ls gs://${staging_meta_info_gcs_prefix}/krb5_prop_acl/* 2>/dev/null || true) | wc -l)"
        if [[ "${finished_count}" == "${#ADDITIONAL_MASTERS[@]}" ]]; then
          krb5_prop_acl_configured=1
          break
        fi
        sleep 1
      done

      if [[ "${krb5_prop_acl_configured}" == 0 ]]; then
        echo 'Slave KDCs have not finished ACL configuration even after 5 minutes, something went wrong.'
        exit 1
      fi

      for additional_master in "${ADDITIONAL_MASTERS[@]}"; do
        kprop -f /etc/krb5kdc/slave_datatrans "${additional_master}"
        touch "${additional_master}.${DOMAIN}-propagated"
        gsutil cp "${additional_master}.${DOMAIN}-propagated" "gs://${staging_meta_info_gcs_prefix}/${additional_master}.${DOMAIN}-propagated"
        rm "${additional_master}.${DOMAIN}-propagated"
      done

      echo 'Preparing krb5_prop script'
      cat << 'EOF' > "${KRB5_PROP_SCRIPT}"
#!/bin/bash
SLAVES=($(/usr/share/google/get_metadata_value attributes/dataproc-master-additional | sed 's/,/\n/g'))
/usr/sbin/kdb5_util dump /etc/krb5kdc/slave_datatrans
for slave in "${SLAVES[@]}"; do
  /usr/sbin/kprop -f /etc/krb5kdc/slave_datatrans "${slave}"
done
EOF
      chmod +x "${KRB5_PROP_SCRIPT}"

      (crontab -l 2>/dev/null || true; echo "*/5 * * * * ${KRB5_PROP_SCRIPT} >/dev/null 2>&1") | crontab -
    fi
  fi
}

function set_property_in_xml() {
  bdconfig set_property \
    --configuration_file $1 \
    --name "$2" --value "$3" \
    --create_if_absent \
    --clobber \
    || err "Unable to set $2"
}

function master_name_to_fqdn_in_file() {
  if [[ "${HA_MODE}" == 1 ]]; then
    for master in "${MASTERS[@]}"; do
      sed -i -e "s/${master}/${master}.${DOMAIN}/g" "$1"
    done
  else
    sed -i -e "s/${MASTER}/${MASTER_FQDN}/g" "$1"
  fi
}

function set_property_core_site() {
  set_property_in_xml "${HADOOP_CONF_DIR}/core-site.xml" "$1" "$2"
}

function config_core_site() {
  echo "Setting properties in core-site.xml."
  master_name_to_fqdn_in_file "${HADOOP_CONF_DIR}/core-site.xml"
  set_property_core_site 'hadoop.security.authentication' 'kerberos'
  set_property_core_site 'hadoop.security.authorization' 'true'
  set_property_core_site 'hadoop.rpc.protection' 'privacy'
  set_property_core_site 'hadoop.ssl.enabled' 'true'
  set_property_core_site 'hadoop.ssl.require.client.cert' 'false'
  set_property_core_site 'hadoop.ssl.hostname.verifier' 'DEFAULT'
  set_property_core_site 'hadoop.ssl.keystores.factory.class' 'org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory'
  set_property_core_site 'hadoop.ssl.server.conf' 'ssl-server.xml'
  set_property_core_site 'hadoop.ssl.client.conf' 'ssl-client.xml'
  # Starting from Hadoop 2.8.4, TLSv1.1, TLSv1.2 and SSLv2Hello are added to the
  # default "hadoop.ssl.enabled.protocols" property. However, conscrypt/BoringSSL
  # (the default SSL implementation used by Dataproc) does not support SSLv2Hello.
  # See: https://github.com/google/conscrypt/blob/master/CAPABILITIES.md
  set_property_core_site 'hadoop.ssl.enabled.protocols' 'TLSv1,TLSv1.1,TLSv1.2'

  # This will map all principals from all realms to the first part of the
  # principal name.
  # eg: both 'my-name/master.fqdn@MY.REALM' and 'my-name@MY.REALM' will be mapped to
  # 'my-name'.
  if [[ -n "${cross_realm_trust_realm}" ]]; then
    auth_to_local_rules=$(cat << 'EOF'
    RULE:[1:$1](.*)s/(.*)/$1/g
    RULE:[2:$1](.*)s/(.*)/$1/g
    DEFAULT
EOF
)
    set_property_core_site 'hadoop.security.auth_to_local' "${auth_to_local_rules}"
  fi
}

function set_property_hdfs_site() {
  set_property_in_xml "${HADOOP_CONF_DIR}/hdfs-site.xml" "$1" "$2"
}

function config_hdfs_site() {
  echo "Setting properties in hdfs-site.xml."
  master_name_to_fqdn_in_file "${HADOOP_CONF_DIR}/hdfs-site.xml"
  set_property_hdfs_site 'dfs.block.access.token.enable' 'true'
  set_property_hdfs_site 'dfs.encrypt.data.transfer' 'true'
  # Name Node
  set_property_hdfs_site 'dfs.namenode.keytab.file' "${HADOOP_CONF_DIR}/hdfs.keytab"
  set_property_hdfs_site 'dfs.namenode.kerberos.principal' "hdfs/_HOST@${REALM}"
  set_property_hdfs_site 'dfs.namenode.kerberos.internal.spnego.principal' "HTTP/_HOST@${REALM}"
  # Secondary Name Node
  set_property_hdfs_site 'dfs.secondary.namenode.keytab.file' "${HADOOP_CONF_DIR}/hdfs.keytab"
  set_property_hdfs_site 'dfs.secondary.namenode.kerberos.principal' "hdfs/_HOST@${REALM}"
  set_property_hdfs_site 'dfs.secondary.namenode.kerberos.internal.spnego.principal' "HTTP/_HOST@${REALM}"
  # Journal Node
  set_property_hdfs_site 'dfs.journalnode.keytab.file' "${HADOOP_CONF_DIR}/hdfs.keytab"
  set_property_hdfs_site 'dfs.journalnode.kerberos.principal' "hdfs/_HOST@${REALM}"
  set_property_hdfs_site 'dfs.journalnode.kerberos.internal.spnego.principal' "HTTP/_HOST@${REALM}"
  # Data Node
  # 'dfs.data.transfer.protection' Enable SASL, only supported after version 2.6
  set_property_hdfs_site 'dfs.data.transfer.protection' 'privacy'
  set_property_hdfs_site 'dfs.datanode.keytab.file' "${HADOOP_CONF_DIR}/hdfs.keytab"
  set_property_hdfs_site 'dfs.datanode.kerberos.principal' "hdfs/_HOST@${REALM}"
  # WebHDFS
  set_property_hdfs_site 'dfs.webhdfs.enabled' 'true'
  set_property_hdfs_site 'dfs.web.authentication.kerberos.principal' "HTTP/_HOST@${REALM}"
  set_property_hdfs_site 'dfs.web.authentication.kerberos.keytab' "${HADOOP_CONF_DIR}/http.keytab"
  # HTTPS
  set_property_hdfs_site 'dfs.http.policy' 'HTTPS_ONLY'
}

function set_property_yarn_site() {
  set_property_in_xml "${HADOOP_CONF_DIR}/yarn-site.xml" "$1" "$2"
}

function config_yarn_site_and_permission() {
  echo "Setting properties in yarn-site.xml."
  master_name_to_fqdn_in_file "${HADOOP_CONF_DIR}/yarn-site.xml"
  # Resource Manager
  set_property_yarn_site 'yarn.resourcemanager.keytab' "${HADOOP_CONF_DIR}/yarn.keytab"
  set_property_yarn_site 'yarn.resourcemanager.principal' "yarn/_HOST@${REALM}"
  # Node Manager
  set_property_yarn_site 'yarn.nodemanager.keytab' "${HADOOP_CONF_DIR}/yarn.keytab"
  set_property_yarn_site 'yarn.nodemanager.principal' "yarn/_HOST@${REALM}"
  if [[ "${HA_MODE}" == 1 ]]; then
    set_property_yarn_site 'yarn.resourcemanager.webapp.https.address.rm0' "${CLUSTER_NAME}-m-0.${DOMAIN}:8090"
    set_property_yarn_site 'yarn.resourcemanager.webapp.https.address.rm1' "${CLUSTER_NAME}-m-1.${DOMAIN}:8090"
    set_property_yarn_site 'yarn.resourcemanager.webapp.https.address.rm2' "${CLUSTER_NAME}-m-2.${DOMAIN}:8090"
  fi
  # Containers need to be run as the users that submitted the job.
  # See https://www.ibm.com/support/knowledgecenter/en/SSPT3X_4.2.0/com.ibm.swg.im.infosphere.biginsights.install.doc/doc/inst_adv_yarn_config.html
  set_property_yarn_site 'yarn.nodemanager.container-executor.class' 'org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor'
  set_property_yarn_site 'yarn.nodemanager.linux-container-executor.group' 'yarn'
  chown yarn /hadoop/yarn/nm-local-dir

  # HTTPS
  set_property_yarn_site 'yarn.http.policy' 'HTTPS_ONLY'
  set_property_yarn_site 'yarn.log.server.url' "https://${MASTER_FQDN}:19889/jobhistory/logs"

  # YARN App Timeline Server
  set_property_yarn_site 'yarn.timeline-service.http-authentication.type' 'kerberos'
  set_property_yarn_site 'yarn.timeline-service.principal' "yarn/_HOST@${REALM}"
  set_property_yarn_site 'yarn.timeline-service.keytab' "${HADOOP_CONF_DIR}/yarn.keytab"
  set_property_yarn_site 'yarn.timeline-service.http-authentication.kerberos.principal' "HTTP/_HOST@${REALM}"
  set_property_yarn_site 'yarn.timeline-service.http-authentication.kerberos.keytab' "${HADOOP_CONF_DIR}/http.keytab"

  cat << EOF > "${HADOOP_CONF_DIR}"/container-executor.cfg
yarn.nodemanager.linux-container-executor.group=yarn
banned.users=hdfs,yarn,mapred,bin
min.user.id=1000
EOF

}

function set_property_mapred_site() {
  set_property_in_xml "${HADOOP_CONF_DIR}/mapred-site.xml" "$1" "$2"
}

function config_mapred_site() {
  echo "Setting properties in mapred-site.xml."
  if [[ "${FQDN}" == "${MASTER_FQDN}" ]]; then
    set_property_mapred_site 'mapreduce.jobhistory.keytab' "${HADOOP_CONF_DIR}/mapred.keytab"
    set_property_mapred_site 'mapreduce.jobhistory.principal' "mapred/_HOST@${REALM}"
  fi

  set_property_mapred_site 'mapreduce.jobhistory.http.policy' 'HTTPS_ONLY'
  set_property_mapred_site 'mapreduce.ssl.enabled' 'true'
  set_property_mapred_site 'mapreduce.shuffle.ssl.enabled' 'true'
  set_property_mapred_site 'mapreduce.jobhistory.address' "${MASTER_FQDN}:10020"
  set_property_mapred_site 'mapreduce.jobhistory.webapp.http.address' "${MASTER_FQDN}:19888"
  set_property_mapred_site 'mapreduce.jobhistory.webapp.https.address' "${MASTER_FQDN}:19889"
}

function copy_or_create_keystore_files() {
  mkdir "${HADOOP_CONF_DIR}"/ssl
  if [[ -n "${keystore_uri}" ]]; then
    echo "Copying keystore and truststore files from GCS."
    gsutil cp "${keystore_uri}" "${HADOOP_CONF_DIR}"/ssl/keystore.jks
    gsutil cp "${truststore_uri}" "${HADOOP_CONF_DIR}"/ssl/truststore.jks
  else
    local keystore_location="${staging_meta_info_gcs_prefix}/keystores"
    if [[ "${FQDN}" == "${MASTER_FQDN}" ]]; then
      echo "Generating self-signed certificate from ${FQDN}"
      # The validity of the cert is 1800 days.
      keytool -genkeypair -keystore keystore.jks -keyalg RSA -keysize 2048 \
        -storepass "${keystore_password}" -keypass "${keystore_password}" -validity 1800 -alias dataproc-cert \
        -dname "CN=*.${DOMAIN}"
      keytool -export -alias dataproc-cert -storepass "${keystore_password}" -file dataproc.cert -keystore keystore.jks
      keytool -importcert -keystore truststore.jks -alias dataproc-cert -storepass "${keystore_password}" -file dataproc.cert -noprompt
      rm dataproc.cert
      gsutil cp keystore.jks truststore.jks "gs://${keystore_location}"
      mv keystore.jks "${HADOOP_CONF_DIR}"/ssl/
      mv truststore.jks "${HADOOP_CONF_DIR}"/ssl/
    else
      local files_downloaded=0
      for (( i=0; i < 5*60; i++ )); do
        echo "Waiting for keystore and truststore files."
        if gsutil -q stat "gs://${keystore_location}/keystore.jks" && gsutil -q stat "gs://${keystore_location}/truststore.jks";  then
          gsutil cp "gs://${keystore_location}/keystore.jks" "${HADOOP_CONF_DIR}"/ssl/keystore.jks
          gsutil cp "gs://${keystore_location}/truststore.jks" "${HADOOP_CONF_DIR}"/ssl/truststore.jks
          files_downloaded=1
          break
        fi
        sleep 1
      done

      if [[ "${files_downloaded}" == 0 ]]; then
        error 'Failed to download keystore and truststore files after 5 minutes.'
      fi
    fi
  fi
  chown -R yarn:hadoop "${HADOOP_CONF_DIR}"/ssl
  chmod 755 "${HADOOP_CONF_DIR}"/ssl
  chmod 440 "${HADOOP_CONF_DIR}"/ssl/keystore.jks
  chmod 444 "${HADOOP_CONF_DIR}"/ssl/truststore.jks
}

function set_property_ssl_xml() {
  set_property_in_xml "${HADOOP_CONF_DIR}/ssl-$1.xml" "$2" "$3"
}

function config_ssl_xml() {
  echo "Setting properties in ssl-$1.xml."
  cp "${HADOOP_CONF_DIR}/ssl-$1.xml.example" "${HADOOP_CONF_DIR}/ssl-$1.xml"
  set_property_ssl_xml "$1" "ssl.$1.truststore.location" "${HADOOP_CONF_DIR}/ssl/truststore.jks"
  set_property_ssl_xml "$1" "ssl.$1.truststore.password" "${keystore_password}"
  set_property_ssl_xml "$1" "ssl.$1.truststore.type" "jks"
  set_property_ssl_xml "$1" "ssl.$1.keystore.location" "${HADOOP_CONF_DIR}/ssl/keystore.jks"
  set_property_ssl_xml "$1" "ssl.$1.keystore.password" "${keystore_password}"
  set_property_ssl_xml "$1" "ssl.$1.keystore.keypassword" "${keystore_password}"
  set_property_ssl_xml "$1" "ssl.$1.keystore.type" "jks"
}

function set_property_hive_site() {
  set_property_in_xml "/etc/hive/conf/hive-site.xml" "$1" "$2"
}

function secure_hive() {
  echo "Securing Hive."
  kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey hive/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "xst -k hive.keytab hive/${FQDN}"
  mv ./hive.keytab /etc/hive/conf/
  chown hive:hive /etc/hive/conf/hive.keytab
  chmod 400 /etc/hive/conf/hive.keytab
  # Add the user 'hive' to group 'hadoop' so it can access the keystore files
  usermod -a -G hadoop hive

  # HiveServer2
  set_property_hive_site 'hive.server2.authentication' 'KERBEROS'
  set_property_hive_site 'hive.server2.authentication.kerberos.principal' "hive/_HOST@${REALM}"
  set_property_hive_site 'hive.server2.authentication.kerberos.keytab' '/etc/hive/conf/hive.keytab'
  set_property_hive_site 'hive.server2.thrift.sasl.qop' 'auth-conf'
  # Hive Metastore
  set_property_hive_site 'hive.metastore.sasl.enabled' 'true'
  set_property_hive_site 'hive.metastore.kerberos.principal' "hive/_HOST@${REALM}"
  set_property_hive_site 'hive.metastore.kerberos.keytab.file' '/etc/hive/conf/hive.keytab'
  # WebUI SSL
  set_property_hive_site 'hive.server2.webui.use.ssl' 'true'
  set_property_hive_site 'hive.server2.webui.keystore.path' "${HADOOP_CONF_DIR}/ssl/keystore.jks"
  set_property_hive_site 'hive.server2.webui.keystore.password' "${keystore_password}"
}

function secure_spark() {
  # Dataproc only runs Spark History Server on 'first' master so we only need
  # to Kerberize it on this master.
  if [[ "${FQDN}" == "${MASTER_FQDN}" ]]; then
    echo "Securing Spark."
    kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey spark/${FQDN}"
    kadmin -p root -w "${root_principal_password}" -q "xst -k spark.keytab spark/${FQDN}"
    mv ./spark.keytab /etc/spark/conf/
    chown spark:spark /etc/spark/conf/spark.keytab
    chmod 400 /etc/spark/conf/spark.keytab
    # Add the user 'spark' to group 'hadoop' so it can access the keystore files
    usermod -a -G hadoop spark
    cat << EOF >> /etc/spark/conf/spark-env.sh
SPARK_HISTORY_OPTS="\$SPARK_HISTORY_OPTS
-Dspark.history.kerberos.enabled=true
-Dspark.history.kerberos.principal=spark/${FQDN}@${REALM}
-Dspark.history.kerberos.keytab=/etc/spark/conf/spark.keytab"
EOF
  fi

  # All the nodes receive the configurations
  sed -i "s/spark.yarn.historyServer.address.*/spark.yarn.historyServer.address https:\/\/${MASTER_FQDN}:18480/" /etc/spark/conf/spark-defaults.conf
  cat << EOF >> /etc/spark/conf/spark-defaults.conf
spark.ssl.protocol tls
spark.ssl.historyServer.enabled true
spark.ssl.trustStore ${HADOOP_CONF_DIR}/ssl/truststore.jks
spark.ssl.keyStore ${HADOOP_CONF_DIR}/ssl/keystore.jks
spark.ssl.trustStorePassword ${keystore_password}
spark.ssl.keyStorePassword ${keystore_password}
EOF
}

function restart_services() {
  if [[ "${ROLE}" == 'Master' ]]; then
    local master_services=('hadoop-hdfs-namenode' 'hadoop-hdfs-secondarynamenode' 'hadoop-hdfs-journalnode' 'hadoop-yarn-resourcemanager' 'hive-server2' 'hive-metastore' 'hadoop-yarn-timelineserver' 'hadoop-mapreduce-historyserver' 'spark-history-server' 'hadoop-hdfs-zkfc')
    for master_service in "${master_services[@]}"; do
      if ( systemctl is-enabled --quiet "${master_service}" ); then
        systemctl restart "${master_service}" || err "Cannot restart service: ${master_service}"
      fi
    done
  fi
  # In single node mode, we run datanode and nodemanager on the master.
  if [[ "${ROLE}" == 'Worker' || "${WORKER_COUNT}" == '0' ]]; then
    if [[ "${early_init}" == 'false' ]]; then
      systemctl restart hadoop-hdfs-datanode || err 'Cannot restart data node'
      systemctl restart hadoop-yarn-nodemanager || err 'Cannot restart node manager'
    fi
  fi
}

function create_dataproc_principal() {
  useradd -d /home/dataproc -m dataproc
  echo "Creating principal dataproc/${FQDN}@${REALM}."
  kadmin -p root -w "${root_principal_password}" -q "addprinc -randkey dataproc/${FQDN}"
  kadmin -p root -w "${root_principal_password}" -q "xst -k dataproc.keytab dataproc/${FQDN}"
  mv dataproc.keytab /etc/dataproc.keytab
  chown dataproc:dataproc /etc/dataproc.keytab
  chmod 400 /etc/dataproc.keytab
}

function krb5_propagate() {
  if [[ "${FQDN}" == "${MASTER_FQDN}" && "${HA_MODE}" == 1 ]]; then
    "${KRB5_PROP_SCRIPT}"
  fi
}

function main() {
  update_apt_get
  install_kerberos
  install_jce
  create_cross_realm_trust_principal_if_necessary
  create_root_user_principal_and_ready_stash
  create_service_principals
  config_krb5_propagate_if_necessary
  config_core_site
  config_hdfs_site
  config_yarn_site_and_permission
  config_mapred_site
  copy_or_create_keystore_files
  config_ssl_xml 'server'
  config_ssl_xml 'client'
  secure_hive
  secure_spark
  restart_services
  create_dataproc_principal
  krb5_propagate
}

main
