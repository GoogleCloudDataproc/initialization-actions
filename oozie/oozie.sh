#!/bin/bash
#
# Copyright 2015,2016,2017,2018,2019,2020,2023 Google LLC and contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Initialization action for installing Apache Oozie on a Google Cloud
# Dataproc cluster. This script will install and configure Oozie to run on the
# master node of a Dataproc cluster. The version of Oozie which is installed
# comes from the BigTop repository.
#
# You can find more information about Oozie at http://oozie.apache.org/
# For more information in init actions and Google Cloud Dataproc see the Cloud
# Dataproc documentation at https://cloud.google.com/dataproc/init-actions
#
# This script should run in under a few minutes

set -euxo pipefail

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
readonly OS_NAME

readonly master_node=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

readonly MAVEN_CENTRAL_URI=https://maven-central.storage-download.googleapis.com/maven2

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

case "${DATAPROC_IMAGE_VERSION}" in
  "1.3" | "1.4" | "1.5" | "2.0" )
    curator_version="2.13.0"
    curator_src="/usr/lib/hadoop/lib"
    ;;
  "2.1" | "2.2")
    curator_version="2.13.0"
    curator_src="/usr/lib/spark/jars"
    ;;
  *)
    echo "unsupported DATAPROC_IMAGE_VERSION: ${DATAPROC_IMAGE_VERSION}" >&2
    exit 1
    ;;
esac

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH
export METADATA_HTTP_PROXY=$(/usr/share/google/get_metadata_value attributes/http-proxy)
export METADATA_EMAIL_SMTP_HOST=$(/usr/share/google/get_metadata_value attributes/email-smtp-host)
export METADATA_EMAIL_FROM_ADDRESS=$(/usr/share/google/get_metadata_value attributes/email-from-address)
export MYSQL_ROOT_USERNAME=$(/usr/share/google/get_metadata_value attributes/mysql-root-username || echo "root")
export OOZIE_DB_NAME=$(/usr/share/google/get_metadata_value attributes/oozie-db-name || echo "oozie")
export OOZIE_DB_USERNAME=$(/usr/share/google/get_metadata_value attributes/oozie-db-username || echo "oozie")
export OOZIE_PASSWORD_SECRET_NAME=$(/usr/share/google/get_metadata_value attributes/oozie-password-secret-name || echo "secret-name")
export OOZIE_PASSWORD_SECRET_VERSION=$(/usr/share/google/get_metadata_value attributes/oozie-password-secret-version || echo 1)
export OOZIE_PASSWORD=$(gcloud secrets versions access --secret ${OOZIE_PASSWORD_SECRET_NAME} ${OOZIE_PASSWORD_SECRET_VERSION} || echo oozie-password)

export http_proxy="${METADATA_HTTP_PROXY}"
export https_proxy="${METADATA_HTTP_PROXY}"
export HTTP_PROXY="${METADATA_HTTP_PROXY}"
export HTTPS_PROXY="${METADATA_HTTP_PROXY}"
export no_proxy=metadata.google.internal
export NO_PROXY=metadata.google.internal
export MYSQL_ROOT_PASSWORD_SECRET_NAME=$(/usr/share/google/get_metadata_value attributes/mysql-root-password-secret-name)
export MYSQL_ROOT_PASSWORD_SECRET_VERSION=$(/usr/share/google/get_metadata_value attributes/mysql-root-password-secret-version || echo 1)
export MYSQL_ROOT_PASSWORD=$(gcloud secrets versions access --secret ${MYSQL_ROOT_PASSWORD_SECRET_NAME} ${MYSQL_ROOT_PASSWORD_SECRET_VERSION} || \
    grep 'password=' /etc/mysql/my.cnf | sed 's/^.*=//' || echo root-password)

NUM_LIVE_DATANODES=0

function remove_old_backports {
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will remove any reference to backports repos older than oldstable

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  oldstable=$(curl -s https://deb.debian.org/debian/dists/oldstable/Release | awk '/^Codename/ {print $2}');
  stable=$(curl -s https://deb.debian.org/debian/dists/stable/Release | awk '/^Codename/ {print $2}');

  matched_files="$(grep -rsil '\-backports' /etc/apt/sources.list*)"
  if [[ -n "$matched_files" ]]; then
    for filename in "$matched_files"; do
      grep -e "$oldstable-backports" -e "$stable-backports" "$filename" || \
        sed -i -e 's/^.*-backports.*$//' "$filename"
    done
  fi
}

function await_hdfs_datanodes() {
  # Wait for HDFS to come online
  tryno=0
  delay=0
  until [[ $tryno -gt 9 || ${NUM_LIVE_DATANODES} -gt 0 ]]; do
    NUM_LIVE_DATANODES=`sudo -u hdfs hdfs dfsadmin -report -live | perl -ne 'print $1 if /^Live.*\((.*)\):/'`
      sleep ${delay}s
      (( tryno=${tryno}+1 ))
      (( delay=${tryno}*5 ))
  done

  if [[ $tryno -gt 9 ]]; then
    echo "hdfs did not come online"
    return -1
  fi
}

function set_oozie_property() {
  local prop_name="$1"
  local prop_val="$2"
  /usr/local/bin/bdconfig set_property \
    --configuration_file '/etc/oozie/conf/oozie-site.xml' \
    --name "${prop_name}" --value "${prop_val}" \
    --clobber
}

function set_hadoop_property() {
  local prop_name="$1"
  local prop_val="$2"
  /usr/local/bin/bdconfig set_property \
    --configuration_file '/etc/hadoop/conf/core-site.xml' \
    --name "${prop_name}" --value "${prop_val}" \
    --clobber
}


function retry_command() {
  local cmd="$1"
  # First retry is immediate
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep $((i * 5))
  done
  return 1
}

function min_version() {
  echo -e "$1\n$2" | sort -r -t'.' -n -k1,1 -k2,2 -k3,3 | tail -n1
}

function configure_ssl() {
  local oozie_home=$(getent passwd oozie home | cut -f 6 -d :)
  local domain=$(hostname -d)
  local keystore_file="${oozie_home}/.keystore"
  local keystore_password="password"
  local truststore_file="${oozie_home}/oozie.truststore"
  local certificate_path="${oozie_home}/certificate.cert"
  local certificate_secret_name=

  if [[ "$(hostname -s)" == "${master_node}" ]]; then
    test -f ${keystore_file} ||\
      sudo -u oozie keytool -genkeypair -alias jetty -file ${keystore_file} \
        -keyalg RSA -dname "CN=*.${domain}" \
        -storepass "${keystore_password}" -keypass "${keystore_password}"

    test -f ${certificate_path} ||\
      sudo -u oozie keytool -exportcert -alias jetty -file "${certificate_path}" \
        -storepass "${keystore_password}"

    test -f ${truststore_file} ||\
      sudo -u oozie keytool -import -noprompt -alias jetty -file "${certificate_path}" \
        -keystore "${truststore_file}" -storepass "${keystore_password}"

    if [[ ${NUM_LIVE_DATANODES} != 0 ]]; then
      retry_command "hdfs dfs -put -f ${certificate_path} /tmp/oozie.certificate"
      retry_command "hdfs dfs -put -f ${keystore_file} /tmp/oozie.keystore"
      retry_command "hdfs dfs -put -f ${truststore_file} /tmp/oozie.truststore"
    fi
  else
    if [[ ${NUM_LIVE_DATANODES} != 0 ]]; then
      echo "Secondary master; attempting to copy SSL files (truststore, keystore, certificate) from HDFS."
      retry_command "hdfs dfs -get /tmp/oozie.truststore ${truststore_file}"
      retry_command "hdfs dfs -get /tmp/oozie.keystore ${keystore_file}"
      retry_command "hdfs dfs -get /tmp/oozie.certificate ${certificate_path}"
    fi
  fi

  # Configure the Oozie client to use the truststore.
  echo "export OOZIE_CLIENT_OPTS='-Djavax.net.ssl.trustStore=${truststore_file}'" >> /usr/lib/oozie/conf/oozie-client-env.sh

  # Configure the Oozie client to use the HTTPS URL.
  echo "export OOZIE_URL='https://$(hostname -f):11443/oozie'" >> /usr/lib/oozie/conf/oozie-client-env.sh

  set_oozie_property 'oozie.https.enabled' 'true'
  set_oozie_property 'oozie.https.keystore.file' "${keystore_file}"
  set_oozie_property 'oozie.https.keystore.pass' "${keystore_password}"
  set_oozie_property 'oozie.https.truststore.file' "${truststore_file}"
}

function install_oozie() {
  local enable_ssl
  enable_ssl=$(/usr/share/google/get_metadata_value attributes/oozie-enable-ssl || echo "false")

  # Upgrade the repository and install Oozie
  if [[ ${OS_NAME} == rocky ]]; then

    # update dnf proxy
    retry_command "dnf -y -v install oozie"

    # unzip does not come pre-installed on the 2.1-rocky8 image
    if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.1" | bc -l) == 1  ]]; then
      retry_command "dnf -y install unzip"
      find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
      cp /usr/lib/hadoop/lib/hadoop-shaded-guava-1.1.1.jar /usr/lib/oozie/lib
    fi

    # add mysql service dependency on oozie service
    sed -i '/^# Required-Start:/ s/$/ mysqld.service/' /etc/init.d/oozie

    # setup symlinks for hadoop jar dependencies
    ln -sf /usr/lib/hadoop/hadoop-common.jar                               \
           /usr/lib/hadoop/hadoop-auth.jar                                 \
           /usr/lib/hadoop/hadoop-annotations.jar                          \
           /usr/lib/hadoop-hdfs/hadoop-hdfs-client.jar                     \
           /usr/lib/hadoop-yarn/hadoop-yarn-common.jar                     \
           /usr/lib/hadoop-yarn/hadoop-yarn-client.jar                     \
           /usr/lib/hadoop-yarn/hadoop-yarn-server-common.jar              \
           /usr/lib/hadoop-yarn/hadoop-yarn-api.jar                        \
           /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-jobclient.jar \
           /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-app.jar       \
           /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-common.jar    \
           /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-core.jar      \
           /usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-shuffle.jar   /usr/lib/oozie/lib/
  elif [[ ${OS_NAME} == ubuntu ]] || [[ ${OS_NAME} == debian ]]; then
    retry_command "apt-get install -y gnupg2 && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B7B3B788A8D3785C"
    retry_command "apt-get update --allow-releaseinfo-change"
    retry_command "apt-get install -q -y oozie oozie-client"
  else
    echo "Unsupported OS: '${OS_NAME}'"
    exit 1
  fi

  # For Oozie, remove Log4j 2 jar not compatible with Log4j 1 that was brought by Hive 2
  find /usr/lib/oozie/lib -name "log4j-1.2-api*.jar" -delete

  # Delete redundant Slf4j backend implementation
  find /usr/lib/oozie/lib -name "slf4j-simple*.jar" -delete
  find /usr/lib/oozie/lib -name "log4j-slf4j-impl*.jar" -delete

  # Redirect Log4j2 logging to Slf4j backend
  local log4j2_version
  log4j2_version=$(
    find /usr/lib/oozie/lib -name "log4j-core*-2.*.jar" | cut -d '/' -f 6 | cut -d '-' -f 3
  )
  log4j2_version=${log4j2_version/.jar/}
  if [[ -n ${log4j2_version} ]]; then
    local log4j2_to_slf4j=log4j-to-slf4j-${log4j2_version}.jar
    local log4j2_to_slf4j_url=${MAVEN_CENTRAL_URI}/org/apache/logging/log4j/log4j-to-slf4j/${log4j2_version}/${log4j2_to_slf4j}
    wget -nv --timeout=30 --tries=5 --retry-connrefused "${log4j2_to_slf4j_url}" -P /usr/lib/oozie/lib
  fi

  # Delete old versions of Jetty jars brought in by dependencies
  find /usr/lib/oozie/ -name "jetty*-6.*.jar" -delete

  local oozie_version
  oozie_version=$(oozie version 2>&1 |
    sed -n 's/.*Oozie[^:]\+:[[:blank:]]\+\([0-9]\+\.[0-9]\.[0-9]\+\+\).*/\1/p' | head -n1)
  if [[ $(min_version '5.0.0' "${oozie_version}") == 5.0.0 ]]; then
    find /usr/lib/oozie/ -name "jetty*-7.*.jar" -delete
  fi

  if [[ "$(hostname -s)" == "${master_node}" ]]; then
    local tmp_dir
    tmp_dir=$(mktemp -d -t oozie-install-XXXX)

    # The ext library is needed to enable the Oozie web console
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      http://archive.cloudera.com/gplextras/misc/ext-2.2.zip -P "${tmp_dir}"
    unzip -o -q "${tmp_dir}/ext-2.2.zip" -d /var/lib/oozie

    # Install share lib
    tar -xzf /usr/lib/oozie/oozie-sharelib.tar.gz -C "${tmp_dir}"

    if [[ $(min_version '5.0.0' "${oozie_version}") != 5.0.0 ]]; then
      # Workaround to issue where jackson 1.8 and 1.9 jars are found on the classpath, causing
      # AbstractMethodError at runtime. We know hadoop/lib has matching vesions of jackson.
      rm -f "${tmp_dir}"/share/lib/hive2/jackson-*
      cp /usr/lib/hadoop/lib/jackson-* "${tmp_dir}/share/lib/hive2/"
    fi

    if ! hdfs dfs -test -d "/user/oozie"; then
      await_hdfs_datanodes

      if [[ ${NUM_LIVE_DATANODES} != 0 ]]; then
        hadoop fs -mkdir -p /user/oozie/
        hadoop fs -put -f "${tmp_dir}/share" /user/oozie/

        if grep '^dataproc' /etc/passwd ; then
          local hdfs_username=dataproc
        else
          local hdfs_username=hdfs
        fi

        sudo -u ${hdfs_username} hadoop fs -chown oozie /user/oozie
      fi
    fi

    # Clean up temporary fles
    rm -rf "${tmp_dir}"
  fi

  # Link the MySQL JDBC driver to the Oozie library directory
  ln -sf /usr/share/java/mysql.jar /usr/lib/oozie/lib/mysql.jar

  # Set JDBC properties
  mysql_host=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

  if [[ "${enable_ssl}" == 'true' ]]; then
    configure_ssl
  fi

  set_oozie_property 'oozie.service.JPAService.jdbc.driver' "com.mysql.cj.jdbc.Driver"
  set_oozie_property 'oozie.service.JPAService.jdbc.url' "jdbc:mysql://${mysql_host}/oozie"
  set_oozie_property 'oozie.service.JPAService.jdbc.username' "oozie"
  set_oozie_property 'oozie.service.JPAService.jdbc.password' "${OOZIE_PASSWORD}"
  set_oozie_property 'oozie.email.smtp.host' "${METADATA_EMAIL_SMTP_HOST}"
  set_oozie_property 'oozie.email.from.address' "${METADATA_EMAIL_FROM_ADDRESS}"
  set_oozie_property 'oozie.action.max.output.data' "20000"
  # Set hostname to allow connection from other hosts (not only localhost)
  set_oozie_property 'oozie.http.hostname' "$(hostname -s)"
  # Following property was requested in customer case
  set_oozie_property 'oozie.service.WorkflowAppService.WorkflowDefinitionMaxLength' "1500000"
  # Following 2 properties added for customer case
  set_oozie_property 'oozie.service.URIHandlerService.uri.handlers' "org.apache.oozie.dependency.FSURIHandler,org.apache.oozie.dependency.HCatURIHandler"
  set_oozie_property 'oozie.credentials.credentialclasses' \
    "hcat=org.apache.oozie.action.hadoop.HCatCredentials,hive2=org.apache.oozie.action.hadoop.Hive2Credentials,hbase=org.apache.oozie.action.hadoop.HbaseCredentials"
  # Following 4 properties provided by customer platform team for CEAM - Oozie to HCat integration
  set_oozie_property 'oozie.services.ext' "org.apache.oozie.service.PartitionDependencyManagerService,org.apache.oozie.service.HCatAccessorService"
  set_oozie_property 'oozie.service.HCatAccessorService.hcat.configuration' "/etc/hive/conf.dist/hive-site.xml"
  set_oozie_property 'oozie.service.coord.input.check.requeue.interval' "120000"
  set_oozie_property 'oozie.service.coord.push.check.requeue.interval' "120000"
  # Following properties were added for materialization issues observed in the NDL data lake
  set_oozie_property 'oozie.service.PurgeService.purge.interval' "86400"
  set_oozie_property 'oozie.service.CallableQueueService.threads' "100"
  set_oozie_property 'oozie.service.CallableQueueService.callable.concurrency' "50"
  set_oozie_property 'oozie.service.CoordMaterializeTriggerService.lookup.interval' "300"
  set_oozie_property 'oozie.service.CoordMaterializeTriggerService.scheduling.interval' "60"
  set_oozie_property 'oozie.service.CoordMaterializeTriggerService.materialization.window' "1500"
  set_oozie_property 'oozie.service.CoordMaterializeTriggerService.callable.batch.size' "10"
  set_oozie_property 'oozie.service.CoordMaterializeTriggerService.materialization.system.limit' "150"
  set_oozie_property 'oozie.service.JPAService.pool.max.active.conn' "50"
  set_oozie_property 'oozie.service.StatusTransitService.backward.support.for.states.without.error' "false"
  set_oozie_property 'oozie.service.ActionCheckerService.action.check.delay' "300"
  set_oozie_property 'oozie.action.retry.policy' "exponential"

  # Hadoop must allow impersonation for Oozie to work properly
  set_hadoop_property 'hadoop.proxyuser.oozie.groups' '*'
  set_hadoop_property 'hadoop.proxyuser.oozie.hosts' '*'
  set_oozie_property 'oozie.service.HadoopAccessorService.supported.filesystems' 'hdfs,gs'
  set_oozie_property 'fs.AbstractFileSystem.gs.impl' 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS'

  # https://biconsult.ru/files/new2/Data%20Lake%20for%20Enterprises.pdf page 784
  set_oozie_property 'oozie.service.ProxyUserService.proxyuser.oozie.hosts' '*'
  set_oozie_property 'oozie.service.ProxyUserService.proxyuser.oozie.groups' '*'

  if [[ "$(hostname -s)" == "${master_node}" ]]; then
    # Create the Oozie user in MySQL. Do this before the copies, since other
    # masters may start up and attempt to connect before the HDFS copies
    # below complete. The other masters need to be able to connect to MySQL.
    retry_command "/usr/bin/mysql -u ${MYSQL_ROOT_USERNAME} --password='${MYSQL_ROOT_PASSWORD}' -e 'use ${OOZIE_DB_NAME}' || /usr/bin/mysqladmin -u ${MYSQL_ROOT_USERNAME} --password='${MYSQL_ROOT_PASSWORD}' create ${OOZIE_DB_NAME}"

    /usr/bin/mysql -u ${MYSQL_ROOT_USERNAME} --password="${MYSQL_ROOT_PASSWORD}" <<EOM
CREATE USER IF NOT EXISTS '${OOZIE_DB_USERNAME}'@'%' IDENTIFIED BY '${OOZIE_PASSWORD}';
GRANT ALL PRIVILEGES ON ${OOZIE_DB_NAME}.* TO '${OOZIE_DB_USERNAME}'@'%';
FLUSH PRIVILEGES;
EOM
  fi

  if [[ "$(hostname -s)" == "${master_node}" ]]; then
    local tmp_dir
    tmp_dir=$(mktemp -d -t oozie-install-XXXX)

    # The ext library is needed to enable the Oozie web console
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
      http://archive.cloudera.com/gplextras/misc/ext-2.2.zip -P "${tmp_dir}"
    unzip -o -q "${tmp_dir}/ext-2.2.zip" -d /var/lib/oozie

    # Install share lib
    tar -xzf /usr/lib/oozie/oozie-sharelib.tar.gz -C "${tmp_dir}"

    if [[ $(min_version '5.0.0' "${oozie_version}") != 5.0.0 ]]; then
      # Workaround to issue where jackson 1.8 and 1.9 jars are found on the classpath, causing
      # AbstractMethodError at runtime. We know hadoop/lib has matching vesions of jackson.
      rm -f "${tmp_dir}"/share/lib/hive2/jackson-*
      cp /usr/lib/hadoop/lib/jackson-* "${tmp_dir}/share/lib/hive2/"
    fi

    # start - copy spark and hive dependencies
    local ADDITIONAL_JARS=""
    if [[ ${OS_NAME} == rocky ]]; then
      if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.1" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop-common-*.jar ${tmp_dir}/share/lib/hive/woodstox-core-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/stax2-api-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.5" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar /usr/lib/spark/jars/re2j-1.1.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop-common-*.jar ${tmp_dir}/share/lib/hive/woodstox-core-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/stax2-api-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.4" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop-common-*.jar ${tmp_dir}/share/lib/hive/woodstox-core-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/stax2-api-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        wget -P /usr/lib/oozie/lib ${MAVEN_CENTRAL_URI}/com/google/guava/guava/11.0.2/guava-11.0.2.jar
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 1.3" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
        ADDITIONAL_JARS=""
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        wget -P /usr/lib/oozie/lib ${MAVEN_CENTRAL_URI}/com/google/guava/guava/11.0.2/guava-11.0.2.jar
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.2" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS=""
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        wget -P /usr/lib/oozie/lib ${MAVEN_CENTRAL_URI}/com/google/guava/guava/11.0.2/guava-11.0.2.jar
      else
        echo "unsupported DATAPROC_IMAGE_VERSION: ${DATAPROC_IMAGE_VERSION}" >&2
        exit 1
      fi
    else
      if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.1" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop-common-*.jar ${tmp_dir}/share/lib/hive/woodstox-core-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/stax2-api-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.5" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar /usr/lib/spark/jars/re2j-1.1.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop-common-*.jar ${tmp_dir}/share/lib/hive/woodstox-core-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/stax2-api-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.4" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop-common-*.jar ${tmp_dir}/share/lib/hive/woodstox-core-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/stax2-api-*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        wget -P /usr/lib/oozie/lib ${MAVEN_CENTRAL_URI}/com/google/guava/guava/11.0.2/guava-11.0.2.jar
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.3" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/hadoop*.jar /usr/lib/spark/jars/hadoop*.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/gcs-connector.jar "
        if [[ -f /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar ]]; then
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar "
        else
          ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/*spark-metrics-listener*.jar "
        fi
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.2" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS=""
      else
        echo "unsupported DATAPROC_IMAGE_VERSION: ${DATAPROC_IMAGE_VERSION}" >&2
        exit 1
      fi
    fi

    if [[ ${NUM_LIVE_DATANODES} != 0 ]]; then
      hadoop fs -put -f \
        ${tmp_dir}/share/lib/hive/stax-api-*.jar                \
        ${tmp_dir}/share/lib/hive/commons-*.jar                 \
        /usr/lib/spark/python/lib/py*.zip                       \
        ${ADDITIONAL_JARS}                                      /user/oozie/share/lib/spark
      hadoop fs -put -f /usr/lib/hive/lib/disruptor*.jar                /user/oozie/share/lib/hive
      hadoop fs -put -f /usr/lib/hive/lib/hive-service-*.jar            /user/oozie/share/lib/hive2
      # end - copy spark and hive dependencies

      # For oozie actions, remove log4j from oozie sharelib to allow log4j api classes loaded to avoid conflicts
      res=`hadoop fs -find /user/oozie/share/lib/ -name "log4j-1.2.*"`
      for i in $res
      do
        if [[ $(hadoop fs -find $(dirname "$i") -name "log4j-1.2-api*" | wc -l) -gt 0 ]]; then
          hadoop fs -cp -f $i $i-backup
          hadoop fs -rm $i
        fi
      done
      # Clean up temporary files if datanodes are live
      rm -rf "${tmp_dir}"
    fi
  fi

  if [[ "$(hostname -s)" == "${master_node}" ]]; then
    # Create the Oozie database. Since we are using MySQL,
    # only do this on the master node.
    retry_command "sudo -u oozie /usr/lib/oozie/bin/ooziedb.sh create -run"
  fi

  local gcs_connector_dir="/usr/local/share/google/dataproc/lib"
  if [[ ! -d $gcs_connector_dir ]]; then
    gcs_connector_dir="/usr/lib/hadoop/lib"
  fi
  cp "${gcs_connector_dir}/gcs-connector.jar" /usr/lib/oozie/lib/

  # Detect if current node configuration is HA and then set oozie servers
  local additional_nodes
  additional_nodes=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional |
    sed 's/,/\n/g' | wc -l)
  if [[ ${additional_nodes} -ge 2 ]]; then
    echo 'Starting configuration for HA'
    # List of servers is used for proper zookeeper configuration.
    # It is needed to replace original ports range with specific one
    local servers
    servers=$(grep 'server\.' /usr/lib/zookeeper/conf/zoo.cfg |
      sed 's/server.//g' |
      sed 's/:2888:3888//g' |
      cut -d'=' -f2- |
      sed 's/\n/,/g' |
      head -n 3 |
      sed 's/$/:2181,/g' |
      xargs -L3 |
      sed 's/.$//g')

    /usr/local/bin/bdconfig set_property \
      --configuration_file "/etc/oozie/conf/oozie-site.xml" \
      --name 'oozie.services.ext' --value \
      'org.apache.oozie.service.ZKLocksService,
        org.apache.oozie.service.ZKXLogStreamingService,
        org.apache.oozie.service.ZKJobsConcurrencyService,
        org.apache.oozie.service.ZKUUIDService' \
      --clobber

    /usr/local/bin/bdconfig set_property \
      --configuration_file "/etc/oozie/conf/oozie-site.xml" \
      --name 'oozie.zookeeper.connection.string' --value "${servers}" \
      --clobber
  fi

  # Workaround to avoid classnotfound issues due to old curator jar in Oozie classpath
  if [ -f "/usr/lib/oozie/lib/curator-framework-2.5.0.jar" ]
  then
    find /usr/lib/oozie/lib \
      -name "curator-framework*.jar" -o \
      -name "curator-recipes*.jar" -o \
      -name "curator-client*.jar" \
      -delete
    if [ $(ls ${curator_src}/ | grep "curator.*-${curator_version}.jar" | wc -l) -ne 0 ]; then
      cp ${curator_src}/curator*-${curator_version}.jar /usr/lib/oozie/lib
    fi
  fi

  # Restart the zookeeper service
  if which systemctl > /dev/null && systemctl list-units | grep zookeeper-server > /dev/null ; then
    systemctl restart zookeeper-server
  else
    /usr/lib/zookeeper/bin/zkServer.sh restart
  fi

  # HDFS and YARN must be cycled; restart to clean things up
  for service in hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-yarn-resourcemanager oozie; do
    if [[ $(systemctl list-unit-files | grep ${service}) != '' ]] &&
      [[ $(systemctl is-enabled ${service}) == 'enabled' ]]; then
      systemctl restart ${service}
    fi
  done

  # Leave safe mode - HDFS will enter safe mode because of Name Node restart
  if [[ "$(hostname -s)" == "${master_node}" ]]; then
    case "${DATAPROC_IMAGE_VERSION}" in
      "1.3" | "1.4")
        hadoop dfsadmin -safemode leave
        ;;
      *)
        hdfs dfsadmin -safemode leave
        ;;
    esac
  fi
}

function install_fluentd_configuration() {
  # the /etc/google-fluentd/config.d is not created if the cluster is created with the flag dataproc:dataproc.logging.stackdriver.enable=false
  # enable oozie fluentd only if the directory exists
  if [[ -d /etc/google-fluentd/config.d ]]; then
    cat <<EOF > /etc/google-fluentd/config.d/oozie_fluentd.conf
#################
#
# Oozie
#

# Fluentd config to tail the oozie log files.
# Currently severity is a seperate field from the Cloud Logging log_level.
<source>
  @type tail
  format none
  path /var/log/oozie/*
  pos_file /var/tmp/fluentd.dataproc.oozie.pos
  refresh_interval 2s
  read_from_head true
  tag concat.raw.tail.*
</source>

<match concat.raw.tail.**>
  @type detect_exceptions
  remove_tag_prefix concat
  multiline_flush_interval 0.1
</match>

<filter raw.tail.**>
  @type parser
  key_name message
  <parse>
    @type multi_format
    <pattern>
      format /^((?<time>[^ ]* [^ ]*) *(?<severity>[^ ]*) *(?<class>[^ ]*): (?<message>.*))/
      time_format %Y-%m-%d %H:%M:%S,%L
    </pattern>
    <pattern>
      format /^((?<time>\S+)\s+(?<severity>\S+)\s+\[(?<thread>[^\]]*)\]\s+(?<class>\S+):\s+(?<message>.*))/
      time_format %Y-%m-%dT%H:%M:%S,%L
    </pattern>
    <pattern>
      format none
    </pattern>
  </parse>
</filter>

<match raw.tail.**>
  @type record_reformer
  renew_record false
  enable_ruby true
  auto_typecast true

  # "tag" is transtlated into log in Stackdriver
  # Strip the instance name and .log from the filename if present
  tag \${tag_suffix[-2].sub("-#{Socket.gethostname}", "").sub(/\.log\$/, "")}

  # The following can be used when turning on jobid re-logging:
  # dataproc.googleapis.com/process_id \${job}
  filename \${tag_suffix[-2]}
</match>
EOF

    if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.2" | bc -l) == 1  ]]; then
      systemctl reload-or-restart google-fluentd-docker
    else
      systemctl reload-or-restart google-fluentd
    fi
  else
    echo "Skipped fluentd configuration for oozie."
  fi
}


function main() {
  #Remove debian backports
  if [[ ${OS_NAME} == debian ]] && [[ $(echo "${DATAPROC_IMAGE_VERSION} <= 2.1" | bc -l) == 1 ]]; then
    remove_old_backports
  fi
  # Only run on the master node of the cluster
  if [[ "${ROLE}" == 'Master' ]]; then
    install_oozie
    install_fluentd_configuration
  fi
}

main
