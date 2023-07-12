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

# Use Python from /usr/bin instead of /opt/conda.
export PATH=/usr/bin:$PATH
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

case "${DATAPROC_IMAGE_VERSION}" in
  "1.3" | "1.4" | "1.5" | "2.0" )
    curator_version="2.13.0"
    curator_src="/usr/lib/hadoop/lib"
    ;;
  "2.1")
#    curator_version="5.2.0"
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
export DATAPROC_IMAGE=$(/usr/share/google/get_metadata_value image)
export METADATA_HTTP_PROXY=$(/usr/share/google/get_metadata_value attributes/http-proxy)
export METADATA_EMAIL_SMTP_HOST=$(/usr/share/google/get_metadata_value attributes/email-smtp-host)
export METADATA_EMAIL_FROM_ADDRESS=$(/usr/share/google/get_metadata_value attributes/email-from-address)
export MYSQL_ROOT_USERNAME=$(/usr/share/google/get_metadata_value attributes/mysql-root-username || echo "root")
export OOZIE_DB_NAME=$(/usr/share/google/get_metadata_value attributes/oozie-db-name || echo "oozie")
export OOZIE_DB_USERNAME=$(/usr/share/google/get_metadata_value attributes/oozie-db-username || echo "oozie")
export OOZIE_PASSWORD_SECRET_NAME=$(/usr/share/google/get_metadata_value attributes/oozie-password-secret-name)
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
  local master_node=$(/usr/share/google/get_metadata_value attributes/dataproc-master)

  if [[ "${HOSTNAME}" == "$master_node" ]]; then
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

    retry_command "hdfs dfs -put -f ${certificate_path} /tmp/oozie.certificate"
    retry_command "hdfs dfs -put -f ${keystore_file} /tmp/oozie.keystore"
    retry_command "hdfs dfs -put -f ${truststore_file} /tmp/oozie.truststore"
  else
      echo "Secondary master; attempting to copy SSL files (truststore, keystore, certificate) from HDFS."
      retry_command "hdfs dfs -get /tmp/oozie.truststore ${truststore_file}"
      retry_command "hdfs dfs -get /tmp/oozie.keystore ${keystore_file}"
      retry_command "hdfs dfs -get /tmp/oozie.certificate ${certificate_path}"
  fi

  # Configure the Oozie client to use the truststore.
  echo "export OOZIE_CLIENT_OPTS='-Djavax.net.ssl.trustStore=${truststore_file}'" >> /usr/lib/oozie/conf/oozie-client-env.sh

  # Configure the Oozie client to use the HTTPS URL.
  echo "export OOZIE_URL='https://$(hostname -f):11443/oozie'" >> /usr/lib/oozie/conf/oozie-client-env.sh

  /usr/local/bin/bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.https.enabled' --value 'true' \
      --clobber

  /usr/local/bin/bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.https.keystore.file' --value "${keystore_file}" \
      --clobber

  /usr/local/bin/bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.https.keystore.pass' --value "${keystore_password}" \
      --clobber

  /usr/local/bin/bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.https.truststore.file' --value "${truststore_file}" \
      --clobber
}

function install_oozie() {
  local master_node
  master_node=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
  local enable_ssl
  enable_ssl=$(/usr/share/google/get_metadata_value attributes/oozie-enable-ssl || echo "false")

  # Upgrade the repository and install Oozie
  if [[ ${OS_NAME} == rocky ]]; then
    # unzip does not come pre-installed on the 2.1-rocky8 image
    if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.1" | bc -l) == 1  ]]; then
      retry_command "dnf -y install unzip"
    fi

    # update dnf proxy
    retry_command "dnf -y -v install oozie"

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
    retry_command "apt-get update"
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
    local log4j2_to_slf4j_url=https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-to-slf4j/${log4j2_version}/${log4j2_to_slf4j}
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

  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
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
      hadoop fs -mkdir -p /user/oozie/
      hadoop fs -put -f "${tmp_dir}/share" /user/oozie/
      sudo -u dataproc hadoop fs -chown oozie /user/oozie
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

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.driver' --value "com.mysql.cj.jdbc.Driver" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.url' --value "jdbc:mysql://${mysql_host}/oozie" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.username' --value "oozie" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.jdbc.password' --value "${OOZIE_PASSWORD}" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.email.smtp.host' --value "${METADATA_EMAIL_SMTP_HOST}" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.email.from.address' --value "${METADATA_EMAIL_FROM_ADDRESS}" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.action.max.output.data' --value "20000" \
    --clobber

  # Set hostname to allow connection from other hosts (not only localhost)
  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.http.hostname' --value "${HOSTNAME}" \
    --clobber

  # Following property was requested in customer case
  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.WorkflowAppService.WorkflowDefinitionMaxLength' --value "1500000" \
    --clobber

  # Following 2 properties added for customer case
  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.URIHandlerService.uri.handlers' --value "org.apache.oozie.dependency.FSURIHandler,org.apache.oozie.dependency.HCatURIHandler" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.credentials.credentialclasses' --value "hcat=org.apache.oozie.action.hadoop.HCatCredentials,hive2=org.apache.oozie.action.hadoop.Hive2Credentials,hbase=org.apache.oozie.action.hadoop.HbaseCredentials" \
    --clobber

  # Following 4 properties provided by customer platform team for CEAM - Oozie to HCat integration
  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.services.ext' --value "org.apache.oozie.service.PartitionDependencyManagerService,org.apache.oozie.service.HCatAccessorService" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.HCatAccessorService.hcat.configuration' --value "/etc/hive/conf.dist/hive-site.xml" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.coord.input.check.requeue.interval' --value "120000" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.coord.push.check.requeue.interval' --value "120000" \
    --clobber

  # Following properties were added for materialization issues observed in the NDL data lake
  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.PurgeService.purge.interval' --value "86400" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CallableQueueService.threads' --value "100" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CallableQueueService.callable.concurrency' --value "50" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CoordMaterializeTriggerService.lookup.interval' --value "300" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CoordMaterializeTriggerService.scheduling.interval' --value "60" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CoordMaterializeTriggerService.materialization.window' --value "1500" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CoordMaterializeTriggerService.callable.batch.size' --value "10" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.CoordMaterializeTriggerService.materialization.system.limit' --value "150" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.JPAService.pool.max.active.conn' --value "50" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.StatusTransitService.backward.support.for.states.without.error' --value "false" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
      --name 'oozie.service.ActionCheckerService.action.check.delay' --value "300" \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.action.retry.policy' --value "exponential" \
    --clobber

  # Hadoop must allow impersonation for Oozie to work properly
  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.hosts' --value '*' \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'hadoop.proxyuser.oozie.groups' --value '*' \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/oozie/conf/oozie-site.xml" \
    --name 'oozie.service.HadoopAccessorService.supported.filesystems' --value 'hdfs,gs' \
    --clobber

  /usr/local/bin/bdconfig set_property \
    --configuration_file "/etc/hadoop/conf/core-site.xml" \
    --name 'fs.AbstractFileSystem.gs.impl' \
    --value 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS' \
    --clobber

  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
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

  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
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
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        cp /usr/lib/hive/lib/guava-*.jar /usr/lib/oozie/lib
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.5" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar /usr/lib/spark/jars/re2j-1.1.jar "
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
      else
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        wget -P /usr/lib/oozie/lib https://repo1.maven.org/maven2/com/google/guava/guava/11.0.2/guava-11.0.2.jar
      fi
    else
      if [[ $(echo "${DATAPROC_IMAGE_VERSION} >= 2.1" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar"
      elif [[ $(echo "${DATAPROC_IMAGE_VERSION} > 1.5" | bc -l) == 1  ]]; then
        ADDITIONAL_JARS="${ADDITIONAL_JARS} /usr/lib/spark/jars/spark-hadoop-cloud*.jar  /usr/lib/spark/jars/hadoop-cloud-storage-*.jar /usr/lib/spark/jars/re2j-1.1.jar"
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
      else
        ADDITIONAL_JARS="${ADDITIONAL_JARS} ${tmp_dir}/share/lib/hive/htrace-core4-*-incubating.jar "
        find /usr/lib/oozie/lib/ -name 'guava*.jar' -delete
        wget -P /usr/lib/oozie/lib https://repo1.maven.org/maven2/com/google/guava/guava/11.0.2/guava-11.0.2.jar
      fi
    fi

    hadoop fs -put -f \
      ${tmp_dir}/share/lib/hive/hadoop-common-*.jar           \
      ${tmp_dir}/share/lib/hive/woodstox-core-*.jar           \
      ${tmp_dir}/share/lib/hive/stax-api-*.jar                \
      ${tmp_dir}/share/lib/hive/stax2-api-*.jar               \
      ${tmp_dir}/share/lib/hive/commons-*.jar                 \
      ${tmp_dir}/share/lib/hive/hadoop*.jar                   \
      /usr/lib/spark/jars/hadoop*.jar                         \
      /usr/local/share/google/dataproc/lib/gcs-connector.jar  \
      /usr/lib/spark/python/lib/py*.zip                       \
      ${ADDITIONAL_JARS}                                      \
      /usr/local/share/google/dataproc/lib/spark-metrics-listener.jar /user/oozie/share/lib/spark
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
    # Clean up temporary files
    rm -rf "${tmp_dir}"
  fi

  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
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
    cp ${curator_src}/curator*-${curator_version}.jar /usr/lib/oozie/lib
  fi

  /usr/lib/zookeeper/bin/zkServer.sh restart

  # HDFS and YARN must be cycled; restart to clean things up
  for service in hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-yarn-resourcemanager oozie; do
    if [[ $(systemctl list-unit-files | grep ${service}) != '' ]] &&
      [[ $(systemctl is-enabled ${service}) == 'enabled' ]]; then
      systemctl restart ${service}
    fi
  done

  # Leave safe mode - HDFS will enter safe mode because of Name Node restart
  if [[ "${HOSTNAME}" == "${master_node}" ]]; then
    case "${DATAPROC_IMAGE_VERSION}" in
      "1.3" | "1.4")
        hadoop dfsadmin -safemode leave
        ;;
      "1.5" | "2.0" | "2.1")
        hdfs dfsadmin -safemode leave
        ;;
      *)
        echo "unsupported DATAPROC_IMAGE_VERSION: ${DATAPROC_IMAGE_VERSION}" >&2
        exit 1
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
  path /var/log/oozie/*.log
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

    systemctl reload google-fluentd
  else
    echo "Skipped fluentd configuration for oozie."
  fi
}


function main() {
  # Only run on the master node of the cluster
  if [[ "${ROLE}" == 'Master' ]]; then
    install_oozie
    install_fluentd_configuration
  fi
}

main
