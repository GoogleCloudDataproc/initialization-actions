#!/bin/bash

# Copyright 2019 Google, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This init script installs Apache Knox on the master node of a Cloud
# Dataproc cluster.

set -euxo pipefail

readonly KNOX_GW_CONFIG_GCS="$(/usr/share/google/get_metadata_value attributes/knox-gw-config)"
readonly KNOX_GW_CONFIG="$(sudo -u knox mktemp -d -t knox-init-action-config-XXXX)"

readonly KNOX_HOME=/usr/lib/knox

function os_id()       { grep '^ID=' /etc/os-release | cut -d= -f2 | xargs ; }
function os_version()  { grep '^VERSION_ID=' /etc/os-release | cut -d= -f2 | xargs ; }
function os_codename() { grep '^VERSION_CODENAME=' /etc/os-release | cut -d= -f2 | xargs ; }
function is_rocky()    { [[ "$(os_id)" == 'rocky' ]] ; }
function is_rocky8()   { is_rocky && [[ "$(os_version)" == '8'* ]] ; }
function is_rocky9()   { is_rocky && [[ "$(os_version)" == '9'* ]] ; }
function is_ubuntu()   { [[ "$(os_id)" == 'ubuntu' ]] ; }
function is_ubuntu18() { is_ubuntu && [[ "$(os_version)" == '18.04'* ]] ; }
function is_ubuntu20() { is_ubuntu && [[ "$(os_version)" == '20.04'* ]] ; }
function is_ubuntu22() { is_ubuntu && [[ "$(os_version)" == '22.04'* ]] ; }
function is_debian()   { [[ "$(os_id)" == 'debian' ]] ; }
function is_debian10() { is_debian && [[ "$(os_version)" == '10'* ]] ; }
function is_debian11() { is_debian && [[ "$(os_version)" == '11'* ]] ; }
function is_debian12() { is_debian && [[ "$(os_version)" == '12'* ]] ; }
function os_vercat() { if   is_ubuntu ; then os_version | sed -e 's/[^0-9]//g'
                       elif is_rocky  ; then os_version | sed -e 's/[^0-9].*$//g'
                                        else os_version ; fi ; }

function remove_old_backports {
  if is_debian12 ; then return ; fi
  if is_debian11 ; then return ; fi
  # This script uses 'apt-get update' and is therefore potentially dependent on
  # backports repositories which have been archived.  In order to mitigate this
  # problem, we will use archive.debian.org for the oldoldstable repo

  # https://github.com/GoogleCloudDataproc/initialization-actions/issues/1157
  debdists="https://deb.debian.org/debian/dists"
  oldoldstable=$(curl -s "${debdists}/oldoldstable/Release" | awk '/^Codename/ {print $2}');
  oldstable=$(   curl -s "${debdists}/oldstable/Release"    | awk '/^Codename/ {print $2}');
  stable=$(      curl -s "${debdists}/stable/Release"       | awk '/^Codename/ {print $2}');

  matched_files=( $(test -d /etc/apt && grep -rsil '\-backports' /etc/apt/sources.list*||:) )

  for filename in "${matched_files[@]}"; do
    # Fetch from archive.debian.org for ${oldoldstable}-backports
    perl -pi -e "s{^(deb[^\s]*) https?://[^/]+/debian ${oldoldstable}-backports }
                  {\$1 https://archive.debian.org/debian ${oldoldstable}-backports }g" "${filename}"
  done
  sed -i "s/^deb*/#deb/g" /etc/apt/sources.list.d/google-cloud-logging.list
}

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*" >&2
  exit 1
}

function retry_command() {
  local -r cmd="${1}"
  for ((i = 0; i < 10; i++)); do
    if eval "${cmd}"; then
      return 0
    fi
    sleep 5
  done
  err "Failed to run: '${cmd}'"
}

function install_dependencies() {
  retry_command 'apt install -y knox jq rsync'
  retry_command 'pip install yq'
}

function enable_demo_ldap_for_dev_testing() {
  # We enable demo ldap service that comes with knox installation to
  # ease development. For production, you must replace demo ldap with your own ldap.
  if [[ ! -f "/lib/systemd/system/knoxldapdemo.service" ]] &&
    [[ -f ${KNOX_GW_CONFIG}/services/knoxldapdemo.service ]]; then
    cp "${KNOX_GW_CONFIG}/services/"* "/lib/systemd/system/"
    systemctl daemon-reload
    systemctl enable knoxldapdemo.service
    systemctl reenable knox.service
  fi
}

function read_from_config_file_or_metadata() {
  local -r config_parameter="$1"
  local temp
  temp=$(/usr/share/google/get_metadata_value "attributes/${config_parameter}")
  if [[ -z "${temp}" ]]; then
    temp=$(yq -r ".${config_parameter}" "${KNOX_GW_CONFIG}/knox-config.yaml" | grep -v null)
  fi
  echo "${temp}"
}

function get_config_parameters() {
  # COLLECT CONFIGS
  master_key=$(read_from_config_file_or_metadata "master_key")
  [[ -z "${master_key}" ]] && err "you have to pass a master key"
  generate_cert=$(read_from_config_file_or_metadata "generate_cert")
  echo "generate_cert=${generate_cert}"
  certificate_hostname=$(read_from_config_file_or_metadata "certificate_hostname")
  echo "certificate_hostname=${certificate_hostname}"
  custom_cert_name=$(read_from_config_file_or_metadata "custom_cert_name")
  echo "custom_cert_name: ${custom_cert_name}"
  config_update_interval=$(read_from_config_file_or_metadata "config_update_interval")
}

# Deletes all the existing credentials in keystores so it is best to re-add secrets if you have any
function replace_the_master_key() {
  systemctl stop knox
  [[ -f "/lib/systemd/system/knoxldapdemo.service" ]] && systemctl stop knoxldapdemo
  sudo -u knox "${KNOX_HOME}/bin/knoxcli.sh" create-master --force --master "${master_key}"
  # we delete existing keystores. If you have existing credentials, you should re-add them.
  # You may add the credentials into config file and add via knox CLI.
  rm -rf "${KNOX_HOME}/data/security/keystores/"*
  systemctl start knox
}

function generate_or_replace_certificate() {
  if [[ "$generate_cert" == "true" ]]; then
    # set the hostname to machine name id reserved word HOSTNAME is passed
    [[ "${certificate_hostname}" == "HOSTNAME" ]] && certificate_hostname=$(hostname -A | tr -d '[:space:]')
    sudo -u knox "${KNOX_HOME}/bin/knoxcli.sh" create-cert --hostname "${certificate_hostname}"

    # we have not used export cert command for JKS since we wanted to provide our own storepass, which is the master key
    sudo -u knox keytool -export -alias "gateway-identity" \
      -file "${KNOX_HOME}/data/security/keystores/gateway-client.crt" \
      -keystore "${KNOX_HOME}/data/security/keystores/gateway.jks" \
      -storepass "${master_key}" \
      -noprompt
    sudo -u knox keytool -importcert -alias "gateway-identity" \
      -file ${KNOX_HOME}/data/security/keystores/gateway-client.crt \
      -keystore "${KNOX_HOME}/data/security/keystores/gateway-client.jks" \
      -storepass "${master_key}" \
      -noprompt

    sudo -u knox "${KNOX_HOME}/bin/knoxcli.sh" export-cert --type PEM

    gsutil cp "${KNOX_HOME}/data/security/keystores/gateway-identity.pem" "${KNOX_GW_CONFIG_GCS}/exported-certs/"
    gsutil cp "${KNOX_HOME}/data/security/keystores/gateway-client.jks" "${KNOX_GW_CONFIG_GCS}/exported-certs/"

  elif [[ -n "${custom_cert_name}" ]]; then
    sudo -u knox cp "${KNOX_GW_CONFIG}/${custom_cert_name}" "${KNOX_HOME}/data/security/keystores/gateway.jks"
  fi
}

function initialize_crontab() {
  echo "$config_update_interval /bin/bash $(readlink -f "${0}") update > /var/log/knox-cron.log 2>&1" | crontab -
}

########### INSTALL #############

function install() {
  local master_name
  master_name=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
  if [[ "${master_name}" == $(hostname -s) ]]; then
         [[ -z "${KNOX_GW_CONFIG_GCS}" ]] && err "Metadata knox-gw-config is not provided. knox-gw-config stores the configuration files for knox."
          
          remove_old_backports
          retry_command "apt-get update"
          install_dependencies
          update
          enable_demo_ldap_for_dev_testing
          get_config_parameters
          replace_the_master_key
          generate_or_replace_certificate
          initialize_crontab

         if [[ -f "/lib/systemd/system/knoxldapdemo.service" ]]; then
           systemctl restart knoxldapdemo
         fi
         systemctl restart knox
  fi
  }

########### UPDATE ############

has_gateway_config_changed=""
function update() {
  gsutil -m rsync -d -r "${KNOX_GW_CONFIG_GCS}" "${KNOX_GW_CONFIG}"
  [[ $? -eq 1 ]] && err "Failed to download configurations from the '${KNOX_GW_CONFIG_GCS}'"

  # Updates topologies. No need to restart Knox since it applies the changes automatically.
  rsync -avh --delete "${KNOX_GW_CONFIG}/topologies/" "/etc/knox/conf/topologies/"

  # Update gateway-site.xml
  if [[ -f "${KNOX_GW_CONFIG}/gateway-site.xml" ]]; then
    has_gateway_config_changed="$(rsync -avh "${KNOX_GW_CONFIG}/gateway-site.xml" "/etc/knox/conf/gateway-site.xml" | grep gateway-site.xml || true)"
  fi
}

########### MAIN ################

if [[ $# -gt 0 && "${1}" == "update" ]]; then
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: checking for updates"
  update
  [[ -n "$has_gateway_config_changed" ]] && systemctl restart knox
else
  install
fi