#!/bin/bash
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
# This script installs Hue on a master node within a Google Cloud Dataproc cluster.

set -euxo pipefail

# Determine the role of this node
MASTER_HOSTNAME=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
readonly MASTER_HOSTNAME

OS_NAME=$(grep '^ID=' /etc/os-release | cut -d= -f2 | xargs)
readonly OS_NAME

readonly REGIONAL_ARTIFACTS_BUCKET_PREFIX="gs://goog-dataproc-artifacts-"
readonly GLOBAL_ARTIFACTS_BUCKET="gs://hadoop-lib"
readonly HUE_PYTHON_GCS_SUBDIR="hue/python3.9"

function os_id() {
  grep '^ID=' /etc/os-release | cut -d= -f2 | xargs
}

function os_version() {
  grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs
}

function is_debian() {
  [[ "$(os_id)" == 'debian' ]]
}

function is_debian10() {
  is_debian && [[ "$(os_version)" == '10'* ]]
}

function is_debian11() {
  is_debian && [[ "$(os_version)" == '11'* ]]
}

function is_ubuntu() {
  [[ "$(os_id)" == 'ubuntu' ]]
}

function random_string() {
  tr </dev/urandom -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function retry_command() {
  local cmd="$1"
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      return 0
    fi
    sleep 5
  done
  return 1
}

function replace_backports_repo() {
  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157

  # This script uses 'apt-get update' and is therefore potentially
  # dependent on backports repositories which have been archived.  In
  # order to mitigate this problem, we will replace any reference to
  # backports repos older than oldstable with the correct link

  sudo sed -i 's/^.*debian buster-backports main.*$//g' /etc/apt/sources.list
  if is_debian10 ; then
    echo "deb https://archive.debian.org/debian buster-backports main" >> /etc/apt/sources.list
    echo "deb-src https://archive.debian.org/debian buster-backports main" >> /etc/apt/sources.list
  elif is_debian11 ; then
    local backports_files=( $(grep -rsil 'bullseye-backports' /etc/apt/sources.list*||:) )
    for filename in "${backports_files[@]}"; do
      sudo sed -i 's|https\?://[^/]\+/debian bullseye-backports|https://archive.debian.org/debian bullseye-backports|g' "${filename}"
    done
    local security_files=( $(grep -rsil 'bullseye-security' /etc/apt/sources.list*||:) )
    if [[ ${#security_files[@]} -gt 0 ]]; then
      local security_mirror="https://snapshot.debian.org/archive/debian-security/20260830T000000Z"
      if curl -fsSLI --connect-timeout 5 --max-time 10 "https://archive.debian.org/debian-security/dists/bullseye-security/InRelease" >/dev/null 2>&1; then
        security_mirror="https://archive.debian.org/debian-security"
      fi
      for filename in "${security_files[@]}"; do
        sudo sed -i "s|https\?://[^/]\+/debian-security|${security_mirror}|g" "${filename}"
      done
      echo 'Acquire::Check-Valid-Until "false";' | sudo tee /etc/apt/apt.conf.d/99no-check-valid-until >/dev/null
    fi
  fi
}

function update_repo() {
  if is_debian || is_ubuntu ; then
    retry_command "apt-get update -qq"
  else
    retry_command "yum check-update"
  fi
}

function install_packages() {
  local -r packages="$*"
  if test -d /etc/apt && grep -rsiE '(buster|bullseye)-backports|bullseye-security' /etc/apt/sources.list* ; then
    replace_backports_repo
    update_repo
  fi
  if is_debian || is_ubuntu ; then
    # The mysql-community-server package prompts the user to select
    # password encryption type.  we will accept the default by
    # indicating noninteractive as our frontend

    # We should really specify the option with debconf, but I am not
    # certain what the syntax for that is.  We may have some customers
    # who want one type and others who want another.  Here are the
    # docs:

    # https://manpages.ubuntu.com/manpages/trusty/en/man1/debconf-set-selections.1.html

    # Here is a prompt from the debian installer:

    # To retain compatibility with older client software, the default
    # authentication plugin can be set to the legacy value
    # (mysql_native_password)

    # After installation, the default can be changed by setting the
    # default_authentication_plugin server setting.

    local backports_flag=""
    local codename
    codename="$(lsb_release -sc)"
    if grep -rsEq "^[[:space:]]*(deb|Suites:).*${codename}-backports" \
      /etc/apt/sources.list /etc/apt/sources.list.d/ 2>/dev/null; then
      backports_flag="-t ${codename}-backports"
    fi
    retry_command "DEBIAN_FRONTEND=noninteractive apt-get install ${backports_flag} -y ${packages}"
  else
    retry_command "yum install -y ${packages}"
  fi
}

function resolve_python39_os_flavour() {
  local id ver_major arch
  id="$(os_id)"
  ver_major="$(os_version | cut -d. -f1)"
  arch="$(uname -m)"

  case "${id}-${ver_major}-${arch}" in
    debian-12-x86_64)
      echo "debian12"
      ;;
    ubuntu-22-x86_64)
      echo "ubuntu22"
      ;;
    rocky-9-x86_64)
      echo "rocky9"
      ;;
    ubuntu-22-aarch64)
      echo "ubuntu22-arm"
      ;;
    *)
      err "Unsupported OS/architecture '${id}-${ver_major}-${arch}' for Hue Python 3.9 installation"
      ;;
  esac
}

function ensure_python39_runtime_libs() {
  if is_debian; then
    install_packages \
      libbz2-1.0 libffi8 libgdbm6 libgdbm-compat4 liblzma5 libmariadb3 \
      libncursesw6 libreadline8 libsqlite3-0 libssl3 libuuid1 zlib1g ||
      err "Failed to install Python 3.9 runtime libraries on Debian"
  elif is_ubuntu; then
    install_packages \
      libbz2-1.0 libffi8 libgdbm6 libgdbm-compat4 liblzma5 libmysqlclient21 \
      libncursesw6 libreadline8 libsqlite3-0 libssl3 libuuid1 zlib1g ||
      err "Failed to install Python 3.9 runtime libraries on Ubuntu"
  else
    # Rocky Linux 9 AppStream provides libmysqlclient.so.21 (needed by MySQLdb) via mysql-libs.
    install_packages \
      bzip2-libs gdbm-libs libffi libuuid mysql-libs ncurses-libs \
      openssl-libs readline sqlite-libs xz-libs zlib ||
      err "Failed to install Python 3.9 runtime libraries on Rocky Linux"
  fi
}

function install_python39_if_needed() {
  local image_version="${DATAPROC_IMAGE_VERSION:-${DATAPROC_VERSION:-}}"
  if [[ "${image_version}" != "2.2" && "${image_version}" != "2.3" ]]; then
    return 0
  fi

  if [[ -x /usr/local/bin/python3.9 ]] &&
    /usr/local/bin/python3.9 -c "import encodings, sqlite3, ssl" >/dev/null 2>&1; then
    echo "Python 3.9 is already installed at /usr/local/bin/python3.9; skipping archive installation."
    return 0
  fi

  local flavour archive_name custom_url region gcs_base_dir tmp_dir
  flavour="$(resolve_python39_os_flavour)"
  archive_name="python-3.9-${flavour}.tar.gz"
  custom_url="$(/usr/share/google/get_metadata_value attributes/hue-python-archive-url || true)"
  region="$(/usr/share/google/get_metadata_value attributes/dataproc-region || echo 'global')"

  ensure_python39_runtime_libs

  tmp_dir="$(mktemp -d)"
  if [[ -n "${custom_url}" ]]; then
    retry_command "gsutil cp '${custom_url}' '${tmp_dir}/${archive_name}'" ||
      err "Failed to download custom Python 3.9 archive from ${custom_url}"
    if gsutil -q stat "${custom_url}.sha256"; then
      retry_command "gsutil cp '${custom_url}.sha256' '${tmp_dir}/${archive_name}.sha256'" ||
        err "Failed to download ${custom_url}.sha256"
      (
        cd "${tmp_dir}" &&
          awk -v f="${archive_name}" '{print $1 "  " f}' "${archive_name}.sha256" | sha256sum -c -
      ) || err "SHA-256 verification failed for ${custom_url}"
    fi
  else
    if [[ "${region}" != "global" ]] &&
      gsutil -q stat "${REGIONAL_ARTIFACTS_BUCKET_PREFIX}${region}/${HUE_PYTHON_GCS_SUBDIR}/${archive_name}"; then
      gcs_base_dir="${REGIONAL_ARTIFACTS_BUCKET_PREFIX}${region}/${HUE_PYTHON_GCS_SUBDIR}"
    else
      echo "WARNING: Regional Python 3.9 archive not found for region '${region}'; falling back to ${GLOBAL_ARTIFACTS_BUCKET}."
      gcs_base_dir="${GLOBAL_ARTIFACTS_BUCKET}/${HUE_PYTHON_GCS_SUBDIR}"
    fi

    retry_command "gsutil cp '${gcs_base_dir}/${archive_name}' '${tmp_dir}/${archive_name}'" ||
      err "Failed to download ${gcs_base_dir}/${archive_name}"
    retry_command "gsutil cp '${gcs_base_dir}/${archive_name}.sha256' '${tmp_dir}/${archive_name}.sha256'" ||
      err "Failed to download ${gcs_base_dir}/${archive_name}.sha256"
    (
      cd "${tmp_dir}" &&
        awk -v f="${archive_name}" '{print $1 "  " f}' "${archive_name}.sha256" | sha256sum -c -
    ) || err "SHA-256 verification failed for ${archive_name}"
  fi

  tar --no-same-owner -xzf "${tmp_dir}/${archive_name}" -C / ||
    err "Failed to extract ${archive_name}"
  ldconfig
  rm -rf "${tmp_dir}"

  /usr/local/bin/python3.9 -c "import encodings, sqlite3, ssl, MySQLdb" ||
    err "Python 3.9 runtime verification failed after extracting ${archive_name}"
}

function install_libmysqlclient20() {
  pushd /tmp
  if [[ "${OS_NAME}" == "debian" ]]; then
    wget --tries=3 https://repo.mysql.com/apt/debian/pool/mysql-5.7/m/mysql-community/libmysqlclient20_5.7.42-1debian10_amd64.deb
    dpkg -i libmysqlclient20_5.7.42-1debian10_amd64.deb
    rm -f libmysqlclient20_5.7.42-1debian10_amd64.deb
  elif [[ "${OS_NAME}" == "ubuntu" ]]; then
    wget --tries=3 https://repo.mysql.com/apt/ubuntu/pool/mysql-5.7/m/mysql-community/libmysqlclient20_5.7.42-1ubuntu18.04_amd64.deb
    dpkg -i libmysqlclient20_5.7.42-1ubuntu18.04_amd64.deb
    rm -f libmysqlclient20_5.7.42-1ubuntu18.04_amd64.deb
  else
    echo "Skip installing libmysqlclient20 for ${OS_NAME}"
  fi
  popd
}

function install_hue_and_configure() {
  local -r old_hdfs_url='## webhdfs_url\=http:\/\/localhost:50070'
  local new_hdfs_url
  new_hdfs_url="webhdfs_url\=http:\/\/$(hdfs getconf -confKey dfs.namenode.http-address)"
  local -r hue_password='hue-password'
  local -r old_mysql_settings='## engine=sqlite3(\s+)## host=(\s+)## port=(\s+)## user=(\s+)## password='
  local new_mysql_settings
  new_mysql_settings="engine=mysql\$1host=127.0.0.1\$2port=3306\$3user=hue\$4password=${hue_password}"
  local -r hadoop_conf_dir='/etc/hadoop/conf'

  # Ensure Python 3.9 is installed on Dataproc 2.2/2.3 before installing Hue,
  # because Hue package post-install hooks execute app_reg.py under Python 3.9.
  install_python39_if_needed || err "Failed to install Python 3.9 for Hue"

  # Install Hue
  install_packages hue || err "Failed to install Hue"

  # Stop Hue
  systemctl stop hue || err "Hue stop action not performed"

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/core-site.xml" \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.groups'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'dfs.webhdfs.enabled' --value 'true' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set dfs.webhdfs.enabled'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'hadoop.proxyuser.hue.hosts' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.hosts'

  bdconfig set_property \
    --configuration_file "${hadoop_conf_dir}/hdfs-site.xml" \
    --name 'hadoop.proxyuser.hue.groups' --value '*' \
    --create_if_absent \
    --clobber ||
    err 'Unable to set hadoop.proxyuser.hue.groups'

  cat >>/etc/hue/conf/hue.ini <<'EOF'

# Defaults to ${HADOOP_MR1_HOME} or /usr/lib/hadoop-0.20-mapreduce
hadoop_mapred_home=/usr/lib/hadoop-mapreduce
EOF

  # If Oozie installed, add Hue as proxy user for Oozie
  if [[ -f /etc/oozie/conf/oozie-site.xml ]]; then
    bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.hosts' --value '*' \
      --create_if_absent \
      --clobber ||
      err 'Unable to set hadoop.proxyuser.hue.groups'

    bdconfig set_property \
      --configuration_file '/etc/oozie/conf/oozie-site.xml' \
      --name 'oozie.service.ProxyUserService.proxyuser.hue.groups' --value '*' \
      --create_if_absent \
      --clobber ||
      err 'Unable to set hadoop.proxyuser.hue.groups'

    systemctl restart oozie || err "Unable to restart oozie"
  else
    echo "Oozie not installed, skipped configuring Hue as an user proxy"
  fi

  # Uncomment supported interpreters
  local interpreters=(
    hive sql spark pyspark r jar py text markdown pig java spark2 mapreduce solr druid impala
    sqoop1 distcp shell presto
  )
  for interpreter in "${interpreters[@]}"; do
    sed -i "/^ *# \[\[\[${interpreter}\]\]\]/,/^ *$/s/^\( *\)# \?/\1/" /etc/hue/conf/hue.ini
  done

  # Configure webhdfs_url
  sed -i "s/${old_hdfs_url}/${new_hdfs_url}/" /etc/hue/conf/hue.ini

  # Replace localhost in all values with FQDN
  sed -i "s/\(.*=.*\)localhost/\1$(hostname --fqdn)/" /etc/hue/conf/hue.ini

  # Uncomment necessary config keys
  local config_keys=(
    default_from_email django_server_email resourcemanager_host resourcemanager_api_url
    proxy_api_url history_server_api_url spark_history_server_url hive_server_host livy_server_url
    sql_server_host server_url host_ports rest_url oozie_url whitelist hbase_clusters solr_url
    ensemble
  )
  for config_key in "${config_keys[@]}"; do
    sed -i "s/#[# ]*\(${config_key} \?=\)/\1/" /etc/hue/conf/hue.ini
  done

  # Uncomment Sentry configuration
  sed -i "s/#[# ]*\(hostname \?= \?$(hostname --fqdn)\)/\1/" /etc/hue/conf/hue.ini

  # Comment out any duplicate resourcemanager_api_url fields after the first one
  sed -i '0,/resourcemanager_api_url/! s/resourcemanager_api_url/## resourcemanager_api_url/' \
    /etc/hue/conf/hue.ini

  # Make hive warehouse directory
  local -r warehause_dir="/user/hive/warehouse"
  hdfs dfs -mkdir "${warehause_dir}" || hdfs dfs -stat "${warehause_dir}"

  bdconfig set_property \
    --configuration_file '/etc/hue/conf/hive-site.xml' \
    --create_if_absent \
    --name 'hive.metastore.warehouse.dir' --value '/user/hive/warehouse' \
    --clobber

  # Configure Desktop Database to use mysql
  perl -i -0777 -pe "s/${old_mysql_settings}/${new_mysql_settings}/" /etc/hue/conf/hue.ini

  # Comment out sqlite3 configuration
  sed -i 's/engine=sqlite3/## engine=sqlite3/' /etc/hue/conf/hue.ini

  # Set database name to 'hue'
  sed -i 's/name=\/var\/lib\/hue\/desktop.db/name=hue/' /etc/hue/conf/hue.ini

  # Disable logging and set random secret key
  set +x
  sed -i "s/secret_key=.*/secret_key=$(random_string)/" /etc/hue/conf/hue.ini

  db_mysqlroot_password="$(grep 'password=' /etc/mysql/my.cnf | sed 's/^.*=//')"
  if [[ -z "${db_mysqlroot_password}" ]]; then
    db_mysqlroot_password="root-password"
  fi

  # Create a database, give 'hue' user permissions
  mysql -u root --password="${db_mysqlroot_password}" -e "
      CREATE DATABASE hue;
      CREATE USER 'hue'@'localhost' IDENTIFIED BY '${hue_password}';
      GRANT ALL PRIVILEGES ON hue.* TO 'hue'@'localhost';" ||
    err "Unable to create database"
  set -x

  # Hue creates all needed tables
  /usr/lib/hue/build/env/bin/hue syncdb --noinput
  /usr/lib/hue/build/env/bin/hue migrate

  # Restart servers
  systemctl restart hadoop-hdfs-namenode hadoop-yarn-resourcemanager

  # Start Hue
  systemctl start hue
}

# Only run on the master node ("0"-master in HA mode) of the cluster
if [[ "$(hostname -s)" == "${MASTER_HOSTNAME}" ]]; then
  if test -d /etc/apt && grep -rsiE '(buster|bullseye)-backports|bullseye-security' /etc/apt/sources.list* ; then
    replace_backports_repo
  fi
  update_repo || echo "Ignored errors when updating OS repo index"
  # DATAPROC_IMAGE_VERSION is the preferred variable, but it doesn't exist in
  # old images.
  if [[ "${DATAPROC_IMAGE_VERSION:-${DATAPROC_VERSION}}" == "2.0"  ]]; then
    # Hue 4.10 in Dataproc 2.0 depends on libmysqlclient20, but it has been
    # removed from the MySQL apt repo index, so download and install it
    # explicitly.
    install_libmysqlclient20
  fi
  install_hue_and_configure || err "Hue install process failed"
fi
