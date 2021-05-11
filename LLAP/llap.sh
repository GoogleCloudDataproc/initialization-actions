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

apt-get install -y xmlstarlet

readonly NOT_SUPPORTED_MESSAGE="LLAP initialization action is not supported on Dataproc ${DATAPROC_VERSION}."
[[ $DATAPROC_VERSION != 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly LLAP_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_NODE_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly NODE_MANAGER_vCPU=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.nodemanager.resource.cpu-vcores"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly NODE_MANAGER_MEMORY=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.nodemanager.resource.memory-mb"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly YARN_MAX_CONTAINER_MEMORY=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.scheduler.maximum-allocation-mb"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly ADDITIONAL_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)
readonly HAS_SSD=$(/usr/share/google/get_metadata_value attributes/ssd)
readonly NUM_LLAP_NODES=$(/usr/share/google/get_metadata_value attributes/num-llap-nodes)


function pre_flight_checks(){

##check for bad configurations
if [[ "${NUM_LLAP_NODES}" -ge "${WORKER_NODE_COUNT}" ]]; then
    echo "LLAP node count equals total worker count. There are no nodes to support Tez AM's. Please reduce LLAP instance count and re-deploy." && exit 1
fi

###check to see if HA or not
if [[ -n "$ADDITIONAL_MASTER" ]]; then
    IS_HA=true
else
    IS_HA=false
fi
}


##add xml doc tool for editing hadoop configuration files
function configure_yarn_site(){
    echo "configure yarn-site.xml..."

    xmlstarlet edit --inplace --omit-decl \
    --update '//configuration/property[name="yarn.application.classpath"]/value' \
    -x 'concat(.,",\$HADOOP_CONF_DIR,/usr/local/share/google/dataproc/lib/*,/usr/lib/hadoop/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/tez/*,/usr/lib/tez/lib/*")' \
    /etc/hadoop/conf/yarn-site.xml


    if [[ "${NODE_MANAGER_MEMORY}" != "${YARN_MAX_CONTAINER_MEMORY}" ]]; then
	   echo "not configured properly..."
    fi
}

###add configurations to hive-site for LLAP
function configure_hive_site(){

    ##different configuration if HA
    echo "configure hive-site.xml...."
    if [[ -n $IS_HA ]]; then
    xmlstarlet edit --inplace --omit-decl \
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
        /etc/hive/conf/hive-site.xml
    else 
        xmlstarlet edit --inplace --omit-decl \
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

    if [[ -n $IS_HA ]]; then
        xmlstarlet edit --inplace --omit-decl \
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
        /etc/hadoop/conf/core-site.xml
    else
        xmlstarlet edit --inplace --omit-decl \
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
    cp log4j-slf4j-impl-2.10.0.jar /usr/lib/hive/lib
}

##repackage tez_lib_uris and place on HDFS; only need to do this on one node. 
function package_tez_lib_uris(){
	echo "repackage tez lib uris..."
	cp /usr/lib/tez/lib/* /usr/lib/tez
    cp /usr/local/share/google/dataproc/lib/* /usr/lib/tez
	tar -czvf tez.tar.gz /usr/lib/tez
	runuser -l hdfs -c 'hdfs dfs -mkdir /tez'
    sleep 100
	until `hdfs dfs -copyFromLocal tez.tar.gz /tez`; do echo "Retrying"; sleep 10; done
}

#reconfigure tez.lib.uris to point to hdfs rather than local filesytem; run on all nodes so we have compatiable config files
function configure_tez_site_xml() {
    echo "reconfigure tez-site.xml..."
	sed -i 's@file:/usr/lib/tez,file:/usr/lib/tez/lib,file:/usr/local/share/google/dataproc/lib@${fs.defaultFS}/tez/tez.tar.gz@g' /etc/tez/conf/tez-site.xml
}

###add yarn service directory for hive; run only on one node
function add_yarn_service_dir(){
    echo "adding yarn service directory on the hive user..."
    runuser -l hdfs -c 'hdfs dfs -mkdir /user/hive/.yarn'
	runuser -l hdfs -c 'hdfs dfs -mkdir /user/hive/.yarn/package'
	runuser -l hdfs -c 'hdfs dfs -mkdir /user/hive/.yarn/package/LLAP'
	runuser -l hdfs -c 'hdfs dfs -chown hive:hive  /user/hive/.yarn/package/LLAP'
}

# All nodes need to run this. These files 
function replace_core_llap_files() {
	echo "replacing llap files..."
	#wget https://github.com/jster1357/llap/archive/refs/heads/main.zip -O main.zip
	#unzip main.zip
	#cp llap-main/package.py /usr/lib/hive/scripts/llap/yarn/package.py 
	#cp llap-main/runLlapDaemon.sh /usr/lib/hive/scripts/llap/bin/runLlapDaemon.sh
    #cp llap-main/llap_restart.sh /usr/lib/hive/scripts/llap/bin/llap_restart.sh 

	#open missing properties files
	cp /usr/lib/hive/conf/llap-cli-log4j2.properties.template /usr/lib/hive/conf/llap-cli-log4j2.properties
	cp /usr/lib/hive/conf/llap-daemon-log4j2.properties.template /usr/lib/hive/conf/llap-daemon-log4j2.properties

    ##modify file runLlapDaemon.sh
    sed -i '78s/$/:`hadoop classpath`/' /usr/lib/hive/scripts/llap/bin/runLlapDaemon.sh
    
    ##modify file package.py
    sed -i 's/print \"Cannot find input files\"/print(\"Cannot find input files\")/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"Cannot determine the container size\"/print(\"Cannot determine the container size\")/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Running as a child of LlapServiceDriver\" % (strftime(\"%H:%M:%S\", gmtime()))/print("%s Running as a child of LlapServiceDriver" % (strftime("%H:%M:%S", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Running after LlapServiceDriver\" % (strftime(\"%H:%M:%S\", gmtime()))/print(\"%s Running after LlapServiceDriver\" % (strftime(\"%H:%M:%S\", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Prepared the files\" % (strftime(\"%H:%M:%S\", gmtime()))/print(\"%s Prepared the files\" % (strftime(\"%H:%M:%S\", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Packaged the files\" % (strftime(\"%H:%M:%S\", gmtime()))/print(\"%s Packaged the files\" % (strftime(\"%H:%M:%S\", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/0700/0o700/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Prepared %s\/run.sh for running LLAP on YARN\" % (strftime(\"%H:%M:%S\", gmtime()), output)/print(\"%s Prepared %s\/run.sh for running LLAP on YARN\" % (strftime(\"%H:%M:%S\", gmtime()), output))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/long(max_direct_memory)/int(max_direct_memory)/g' /usr/lib/hive/scripts/llap/yarn/package.py



    print \"%s Prepared %s\/run.sh for running LLAP on YARN\" % (strftime(\"%H:%M:%S\", gmtime()), output)
    print(\"%s Prepared %s\/run.sh for running LLAP on YARN\" % (strftime(\"%H:%M:%S\", gmtime()), output))

}

##if the metadata value exists, then we want to configure the local ssd as a caching location.
function configure_SSD_caching_worker(){
    if [[ -n "$HAS_SSD" ]]; then
        echo "ssd"
        mkdir /mnt/1/llap
        chmod 777 /mnt/1/llap
        chown hive /mnt/1/llap
        xmlstarlet edit --inplace --omit-decl \
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap.path" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "/mnt/1/llap"\
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.memory.mode" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "cache"\
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.use.lrfu" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
        /etc/hive/conf/hive-site.xml
    else
        echo "NO SSD to configure..."
    fi
}

##if the metadata value exists, then we want to configure the local ssd as a caching location. We don't need to make any directory changes since master nodes don't 
##run YARN. We do need to ensure that the configuration files are equal across master and worker nodes
function configure_SSD_caching_master(){
    if [[ -n "$HAS_SSD" ]]; then
	   echo "ssd"
	   xmlstarlet edit --inplace --omit-decl \
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.allocator.mmap.path" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "/mnt/1/llap"\
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.memory.mode" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "cache"\
        -s '//configuration' -t elem -n "property" \
        -s '//configuration/property[last()]' -t elem -n "desription" -v "" \
        -s '//configuration/property[last()]' -t elem -n "name" -v "hive.llap.io.use.lrfu" \
        -s '//configuration/property[last()]' -t elem -n "value" -v "true"\
        /etc/hive/conf/hive-site.xml
    else
        echo "NO SSD to configure..."
fi
}

##start LLAP - Master Node
function start_llap(){


	if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then

        ##restart service after all configuration changes
		echo "restart hive server prior..."
		systemctl restart hive-server2.service 

		echo "starting yarn app fastlaunch...."
		yarn app -enableFastLaunch

		echo "Setting Parameters for LLAP start"
		
        ###we want LLAP to have the entire YARN memory allocation for a node; easier to manage
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
			LLAP_HEADROOM=6114
        else
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

 		
        ###if user didn't pass in num llap instances, take worker node count -1
        if [[ -z $NUM_LLAP_NODES ]]; then
            LLAP_INSTANCES=$(expr ${WORKER_NODE_COUNT} - 1) 
        else
            LLAP_INSTANCES=$NUM_LLAP_NODES
        fi 

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
        --skiphadoopversion \
		--directory /tmp/llap_staging \
		--output /tmp/llap_output \
		--loglevel INFO \
		--startImmediately 
	fi
}

##main driver function for the script
function configure_llap(){

if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then
    echo "running primary master config...."
    pre_flight_checks
	package_tez_lib_uris
	add_yarn_service_dir
	configure_yarn_site
	configure_core_site
	configure_hive_site
	configure_SSD_caching_master
	replace_core_llap_files
	get_log4j
	configure_tez_site_xml
    return 0
fi

if [[ "${ROLE}" == "Worker" ]]; then
    echo "running worker config...."
    pre_flight_checks
	configure_yarn_site
	configure_core_site
	configure_hive_site
	configure_SSD_caching_worker
	get_log4j
	configure_tez_site_xml
	replace_core_llap_files
    return 0
fi

if [[ "${ROLE}" == "Master" && "${HOSTNAME}" != "${LLAP_MASTER_FQDN}"  ]]; then
    echo "running master config...."
    pre_flight_checks
    configure_yarn_site
    configure_core_site
    configure_hive_site
    get_log4j
    configure_tez_site_xml
    replace_core_llap_files
    return 0
fi

}

###run llapstatus command to determine if running
function wait_for_llap_ready() {
    if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then
        echo "wait for LLAP to launch...."
        sudo -u hive hive --service llapstatus --name llap0 -w -r 1 -i 5
        echo "LLAP started...."
    else
        echo "skipping...."
    fi
}

echo "Running configuration process...."
configure_llap

echo "Starting LLAP...."
start_llap

echo "Verify full start...."
wait_for_llap_ready

echo "LLAP Setup Complete!"