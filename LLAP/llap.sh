#!/bin/bash

#    Copyright 2021 Google, Inc.
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

set -euxo pipefail

##install xml modifiction tool....
sudo apt-get install -y xmlstarlet

readonly NOT_SUPPORTED_MESSAGE="LLAP initialization action is not supported on Dataproc ${DATAPROC_VERSION}."
[[ $DATAPROC_VERSION != 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly LLAP_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_NODE_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly NODE_MANAGER_vCPU=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.nodemanager.resource.cpu-vcores"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly NODE_MANAGER_MEMORY=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.nodemanager.resource.memory-mb"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly YARN_MAX_CONTAINER_MEMORY=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.scheduler.maximum-allocation-mb"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly LAST_CHAR_MASTER=${LLAP_MASTER_FQDN: -1}
readonly HAS_SSD=$(/usr/share/google/get_metadata_value attributes/ssd)

###check to see if HA or not
if [[ $LAST_CHAR_MASTER == 'm' ]];then
	IS_HA="NO";else
	IS_HA="YES"
fi

##add xml doc tool for editing hadoop configuration files
function configure_yarn_site(){
	echo "configure yarn-site.xml..."

sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "yarn.application.classpath" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "\$HADOOP_CONF_DIR,/usr/local/share/google/dataproc/lib/*,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/tez/*,/usr/lib/tez/lib/*"\
  /etc/hadoop/conf/yarn-site.xml

if [[ "${NODE_MANAGER_MEMORY}" != "${YARN_MAX_CONTAINER_MEMORY}" ]]; then
	echo "not configured properly..."
fi

}

###add configurations to hive-site for LLAP
function configure_hive_site(){

echo "configure hive-site.xml..."
if [[ $IS_HA == "YES" ]];then
sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "the yarn service name for llap" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.daemon.service.hosts" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "@llap0"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "zookeepr namespace for hive interactive server" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.server2.zookeeper.namespace" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "hiveserver2-interactive"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "hive execution mode, default LLAP" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.execution.mode" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "llap"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.execution.mode" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "only"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.enabled" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.server2.enable.doAs" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "false"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.txn.manager" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.support.concurrency" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.alloc.min" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "256Kb"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.compactor.initiator.on" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.compactor.worker.threads" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "1"\
  /etc/hive/conf/hive-site.xml;else 

  sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "the yarn service name for llap" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.daemon.service.hosts" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "@llap0"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "zookeepr namespace for hive interactive server" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.server2.zookeeper.namespace" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "hiveserver2-interactive"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "hive execution mode, default LLAP" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.execution.mode" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "llap"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.execution.mode" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "only"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.enabled" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.server2.enable.doAs" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "false"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.zookeeper.quorum" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "${LLAP_MASTER_FQDN}:2181"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.zookeeper.client.port" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "2181"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.txn.manager" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.support.concurrency" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.alloc.min" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "256Kb"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.compactor.initiator.on" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.compactor.worker.threads" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "1"\
   /etc/hive/conf/hive-site.xml
fi

}

###add configurations to core-site for LLAP
function configure_core_site(){
	echo "configure core-site.xml..."

if [[ $IS_HA == "YES" ]];then
	sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hadoop.registry.zk.quorum" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "\${hadoop.zk.address}"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hadoop.registry.zk.root" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "/registry"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hadoop.registry.rm.enabled" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  /etc/hadoop/conf/core-site.xml;else

  	sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hadoop.registry.zk.quorum" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "${LLAP_MASTER_FQDN}:2181"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hadoop.registry.zk.root" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "/registry"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hadoop.registry.rm.enabled" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  /etc/hadoop/conf/core-site.xml
fi

}


##add missing log4j file on all nodes
function get_log4j() {
	echo "import missing log4j library..."
 	wget -nv --timeout=30 --tries=5 --retry-connrefused \
    	https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/2.10.0/log4j-slf4j-impl-2.10.0.jar
    sudo cp log4j-slf4j-impl-2.10.0.jar /usr/lib/hive/lib
 }

##repackage tez_lib_uris and place on HDFS; only need to do this on one node. 
function package_tez_lib_uris(){
	echo "repackage tez lib uris..."
	cp /usr/lib/tez/lib/* /usr/lib/tez
	cp /usr/local/share/google/dataproc/lib/* /usr/lib/tez
	tar -czvf tez.tar.gz /usr/lib/tez
	sudo -u hdfs hdfs dfs -mkdir /tez
	until `hdfs dfs -put tez.tar.gz /tez`; do echo "Retrying"; sleep 5; done
	
}

#reconfigure tez.lib.uris to point to hdfs rather than local filesytem; run on all nodes so we have compatiable config files
 function configure_tez_site_xml() {
 	echo "reconfigure tez-site.xml..."
	sudo sed -i 's@file:/usr/lib/tez,file:/usr/lib/tez/lib,file:/usr/local/share/google/dataproc/lib@${fs.defaultFS}/tez/tez.tar.gz@g' /etc/tez/conf/tez-site.xml
 }

###add yarn service directory for hive; run only on one node
 function add_yarn_service_dir(){
 	echo "adding yarn service directory on the hive user..."
 	hdfs dfs -mkdir /user/hive/.yarn
	hdfs dfs -mkdir /user/hive/.yarn/package
	hdfs dfs -mkdir /user/hive/.yarn/package/LLAP
	sudo -u hdfs hdfs dfs -chown hive:hive  /user/hive/.yarn/package/LLAP
 }

# All nodes need to run this. These files 
function replace_core_llap_files() {
	echo "replacing llap files..."
	wget https://github.com/jster1357/llap/archive/refs/heads/main.zip
	unzip main.zip
	sudo cp llap-main/package.py /usr/lib/hive/scripts/llap/yarn/package.py 
	sudo cp llap-main/runLlapDaemon.sh /usr/lib/hive/scripts/llap/bin/runLlapDaemon.sh

	#open missing properties files
	sudo cp /usr/lib/hive/conf/llap-cli-log4j2.properties.template /usr/lib/hive/conf/llap-cli-log4j2.properties
	sudo cp /usr/lib/hive/conf/llap-daemon-log4j2.properties.template /usr/lib/hive/conf/llap-daemon-log4j2.properties
}



##if the metadata value exists, then we want to configure the local ssd as a caching location.
function configure_SSD_caching_worker(){
if [[ $HAS_SSD == 'true' ]];then
	echo "ssd"
    sudo mkdir /mnt/1/llap
	sudo chmod 777 /mnt/1/llap
	sudo chown hive /mnt/1/llap
	sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap.path" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "/mnt/1/llap"\
  /etc/hive/conf/hive-site.xml;else
    echo "NO SSD to configure..."
fi

}


##if the metadata value exists, then we want to configure the local ssd as a caching location. We don't need to make any directory changes since master nodes don't 
##run YARN. We do need to ensure that the configuration files are equal across master and worker nodes
function configure_SSD_caching_master(){
if [[ $HAS_SSD == 'true' ]];then
	echo "ssd"
	sudo xmlstarlet edit --inplace --omit-decl \
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
  -s '//configuration' -t elem -n "property" \
  -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
  -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap.path" \
  -s '//configuration/property[last()]' -t elem -n "value" -v "/mnt/1/llap"\
  /etc/hive/conf/hive-site.xml;else
    echo "NO SSD to configure..."
fi

}

##cleanup files that were downloaded during the configuration process
#function cleanup(){
#	sudo rm log4j-slf4j-impl-2.10.0.jar
#	sudo rm main.zip
#	sudo rm -f tez.tar.gz
#}


##start LLAP - Master Node
function start_llap(){


	if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then


    ##restart service after all configuration changes
		echo "restart hive server prior..."
		sudo systemctl restart hive-server2.service 


		echo "starting yarn app fastlaunch...."
		yarn app -enableFastLaunch

		echo "Setting Parameters for LLAP start"
		
    ### we want LLAP to have the entire YARN memory allocation for a node; easier to manage
		LLAP_SIZE=$NODE_MANAGER_MEMORY
		echo "LLAP daemon size: $LLAP_SIZE"

		LLAP_CPU_ALLO=0
		LLAP_MEMORY_ALLO=0
		LLAP_XMX=0
		LLAP_EXECUTORS=0

		###Get the number of exeuctors based on memory; we want to do a rolling calcualtion of the number of exec based on the available yarn mem pool.
		for ((i = 1; i <= $NODE_MANAGER_vCPU; i++)); do
        LLAP_MEMORY_ALLO=$(($i * 4096))
        ###we take into account headroom here to give some space for cache; this is to prevent situations where we have too little headroom
        if (( $LLAP_MEMORY_ALLO < $(expr $NODE_MANAGER_MEMORY - 6114) )); then
                LLAP_EXECUTORS=$i
                LLAP_XMX=$LLAP_MEMORY_ALLO
        fi
		done

		echo "LLAP executors: ${LLAP_EXECUTORS}"
		echo "LLAP xmx memory: ${LLAP_XMX}"

		### 6% of xmx or max 6GB for jvm headroom; calculate headroom and convert to int
		LLAP_XMX_6=$(echo "scale=0;${LLAP_XMX}*.06" |bc)
		LLAP_XMX_6_INT=${LLAP_XMX_6%.*}

		### jvm headroom for the llap executors
		if  (( $LLAP_XMX_6_INT > 6144 )); then
			LLAP_HEADROOM=6114; else
			LLAP_HEADROOM=$LLAP_XMX_6_INT
		fi
		echo "LLAP daemon headroom: ${LLAP_HEADROOM}"

		##cache is whatever is left over after heardroom and executor memory is accounted for
 		LLAP_CACHE=$(expr ${LLAP_SIZE} - ${LLAP_HEADROOM} - ${LLAP_XMX})


 		##if there is no additional room, then no cache will be used
 		if (( $LLAP_CACHE < 0 )); then
 			LLAP_CACHE=0
 		fi
 		echo "LLAP in-memory cache: ${LLAP_CACHE}"

 		###keep one node in reserve for handling the duties of Tez AM
		LLAP_INSTANCES=$(expr ${WORKER_NODE_COUNT} - 1)
		echo "LLAP daemon instances: ${LLAP_INSTANCES}"

		echo "Starting LLAP..."
		sudo -u hive hive --service llap \
		--instances "${LLAP_INSTANCES}" \
		--size "${LLAP_SIZE}"m \
		--executors "${LLAP_EXECUTORS}" \
		--xmx "${LLAP_XMX}"m \
		--cache "${LLAP_CACHE}"m \
		--name llap0 \
		--auxhbase=false \
		--directory /tmp/llap_staging \
		--output /tmp/llap_output \
		--loglevel INFO \
		--startImmediately
	fi
}

##main driver function for the script
function configure_llap(){

 if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then
 	sleep 100
 	package_tez_lib_uris
 	add_yarn_service_dir
 	configure_yarn_site
	configure_core_site
	configure_hive_site
	configure_SSD_caching_master
	replace_core_llap_files
	get_log4j
	configure_tez_site_xml
 fi

if [[ "${ROLE}" == "Worker" ]] || [["${ROLE}" == "Master"]]; then
	configure_yarn_site
	configure_core_site
	configure_hive_site
	configure_SSD_caching_worker
	get_log4j
	configure_tez_site_xml
	replace_core_llap_files
fi
}

###run llapstatus command to determine if running
function wait_for_llap_ready() {
  if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then
    echo "wait for LLAP to launch...."
    sudo -u hive hive --service llapstatus --name llap0 -w -r 1 -i 5
    echo "LLAP started...."; else
    echo "skipping...."
  fi

}

echo "Running configuration process...."
configure_llap

echo "Starting LLAP...."
start_llap

echo "Verify full start...."
wait_for_llap_ready

#echo "cleanup install files...."
#cleanup

echo "LLAP Setup Complete!"