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

# This init script installs Apache Zeppelin on the master node of a Cloud
# Dataproc cluster. Zeppelin is also configured based on the size of your
# cluster and the versions of Spark/Hadoop which are installed.

set -euxo pipefail

readonly KNOX_GW_CONFIG_GS=$(/usr/share/google/get_metadata_value attributes/knox-gw-config-gs)
readonly KNOX_GW_CONFIG=/knox-config
readonly GATEWAY_HOME=/usr/lib/knox

function retry_install_command() {
  cmd="$1"
	local output=1
  for ((i = 0; i < 10; i++)); do
    if eval "$cmd"; then
      output=0
			break
    fi
    sleep 5
  done

	if [ $output -ne 0 ]; then
		err "Failed to install: '$cmd' "
		return 1
	else
		return 0
	fi 
}

function update_apt_get() {
  retry_install_command "apt-get update"
}


function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

function install_dependencies(){
	retry_install_command 'apt install knox jq python-pip -y'
	retry_install_command 'pip install yq'
}

function enable_demo_ldap_for_dev_testing(){
	# We enable demo ldap service that comes with knox installation to 
	# ease development. For production, you must replace demo ldap with your own ldap.
	if [[ ! -f "/lib/systemd/system/knoxldapdemo.service" && -f $KNOX_GW_CONFIG/services/knoxldapdemo.service ]]; then
		cp $KNOX_GW_CONFIG/services/* /lib/systemd/system/
		systemctl daemon-reload
		systemctl enable knoxldapdemo.service
		systemctl reenable knox.service
	fi
}

function read_from_config_file_or_metadata(){
	local config_parameter="$1"
  local temp=$(/usr/share/google/get_metadata_value attributes/$config_parameter)
	if [[ -z "$temp" ]]; then
	  temp=$(yq -r .$config_parameter $KNOX_GW_CONFIG/knox-config.yaml | grep -v null)
	fi
	echo "$temp" 
}

function get_config_parameters(){
	# COLLECT CONFIGS

	master_key=$(read_from_config_file_or_metadata "master_key")
	[[ -z "$master_key" ]] && err "you have to pass a master key"
	generate_cert=$(read_from_config_file_or_metadata "generate_cert")
	echo "generate_cert=${generate_cert}"
	certificate_hostname=$(read_from_config_file_or_metadata "certificate_hostname")
	echo "certificate_hostname=${certificate_hostname}"
	custom_cert_name=$(read_from_config_file_or_metadata "custom_cert_name")
	echo "custom_cert_name: $custom_cert_name"
	config_update_interval=$(read_from_config_file_or_metadata "config_update_interval")
}

	# Deletes all the existing credentials in keystores so it is best to re-add secrets if you have any
function replace_the_master_key(){
	systemctl stop knox
	[[ -f "/lib/systemd/system/knoxldapdemo.service" ]] && sudo systemctl stop knoxldapdemo
	sudo -u knox $GATEWAY_HOME/bin/knoxcli.sh create-master --force --master $master_key
	# we delete existing keystores. If you have existing credentials, you should re-add them. 
	# You may add the credentials into config file and add via knoxcli.
	sudo rm -rf  $GATEWAY_HOME/data/security/keystores/*
	systemctl start knox
}

function generate_or_replace_certificate(){
	if [[ "$generate_cert" == "true" ]]; then
		# set the hostname to machine name id reserved word HOSTNAME is passed
		[[ "$certificate_hostname" == "HOSTNAME" ]] && certificate_hostname=`hostname -A | tr -d '[:space:]'`
		sudo -u knox $GATEWAY_HOME/bin/knoxcli.sh create-cert --hostname $certificate_hostname
	
		# we have not used export cert command for JKS since we wanted to provide our own storepass, which is the master key
		sudo -u knox keytool -export -alias gateway-identity -file $GATEWAY_HOME/data/security/keystores/gateway-client.crt \
		       -keystore $GATEWAY_HOME/data/security/keystores/gateway.jks -storepass $master_key -noprompt
		sudo -u knox keytool -importcert -file $GATEWAY_HOME/data/security/keystores/gateway-client.crt \
		       -keystore $GATEWAY_HOME/data/security/keystores/gateway-client.jks -alias "gateway-identity" -storepass $master_key -noprompt
		
		sudo -u knox $GATEWAY_HOME/bin/knoxcli.sh export-cert --type PEM

		gsutil cp $GATEWAY_HOME/data/security/keystores/gateway-identity.pem gs://$KNOX_GW_CONFIG_GS/exported-certs/
		gsutil cp $GATEWAY_HOME/data/security/keystores/gateway-client.jks gs://$KNOX_GW_CONFIG_GS/exported-certs/

	elif [[ ! -z "${custom_cert_name}" ]]; then
		sudo -u knox cp $KNOX_GW_CONFIG/$custom_cert_name $GATEWAY_HOME/data/security/keystores/gateway.jks
	fi
}

function configure_knox(){
	mkdir -p $KNOX_GW_CONFIG
	chown -R knox:knox $KNOX_GW_CONFIG
  update	
}

function initialize_crontab(){
	echo "$config_update_interval /bin/bash $(readlink -f $0) update > /var/log/knox-cron.log 2>&1" | crontab -
}

########### INSTALL #############

function install(){
	local MASTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-master)
  if [[ "${MASTER_NAME}" == $(hostname) ]]; then
		[[ -z "$KNOX_GW_CONFIG_GS" ]] && err "metadata knox-gw-config-gs is not provided. knox-gw-config-gs stores the configuration files for knox."
		
			
		update_apt_get || err 'Failed to update apt-get'
		install_dependencies
		configure_knox
		enable_demo_ldap_for_dev_testing
    get_config_parameters
    replace_the_master_key
    generate_or_replace_certificate
		initialize_crontab

		[[ -f "/lib/systemd/system/knoxldapdemo.service" ]] && systemctl restart knoxldapdemo
		systemctl restart knox
	fi
}

########### UPDATE ############

has_gateway_config_changed=""
function update(){
	sudo -u knox gsutil rsync -d -r gs://$KNOX_GW_CONFIG_GS $KNOX_GW_CONFIG
	[[ $? -eq 1 ]] && err "Failed to download configurations from the bucket: $KNOX_GW_CONFIG_GS"
	# updates topologies. No need to restart knox since it applies the changes automatically
	rsync -avh --delete $KNOX_GW_CONFIG/topologies/ /etc/knox/conf/topologies/ 
	# update gateway-site.xml

	if [[ -f "$KNOX_GW_CONFIG/gateway-site.xml" ]]; then 
	  has_gateway_config_changed="$(rsync -avh $KNOX_GW_CONFIG/gateway-site.xml /etc/knox/conf/gateway-site.xml | grep gateway-site.xml || :)" 
	fi
}

########### MAIN ################

if [[ $# > 0 && "$1" == "update" ]]; then
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: checking for updates"
  update
	[[ ! -z "$has_gateway_config_changed" ]] && systemctl restart knox
else
  install
fi