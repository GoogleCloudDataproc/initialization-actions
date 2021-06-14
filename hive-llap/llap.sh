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

readonly NOT_SUPPORTED_MESSAGE="LLAP initialization action is not supported on Dataproc ${DATAPROC_VERSION}."
[[ $DATAPROC_VERSION != 2.* ]] && echo "$NOT_SUPPORTED_MESSAGE" && exit 1

# Variables for running this script
readonly ROLE="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
readonly LLAP_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_NODE_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly NODE_MANAGER_vCPU=$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.nodemanager.resource.cpu-vcores)
readonly NODE_MANAGER_MEMORY=$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.nodemanager.resource.memory-mb)
readonly YARN_MAX_CONTAINER_MEMORY=$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.scheduler.maximum-allocation-mb)
readonly ADDITIONAL_MASTER=$(/usr/share/google/get_metadata_value attributes/dataproc-master-additional)
readonly HAS_SSD=$(/usr/share/google/get_metadata_value attributes/ssd)
readonly NUM_LLAP_NODES=$(/usr/share/google/get_metadata_value attributes/num-llap-nodes)
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'
readonly HIVE_CONF_DIR='/etc/hive/conf'
readonly DEFAULT_INIT_ACTIONS_REPO="gs://dataproc-initialization-actions"
readonly EXECUTOR_SIZE=$(/usr/share/google/get_metadata_value attributes/exec_size_mb || echo 4096)

# user supplied location for file to ingest
readonly INIT_ACTIONS_REPO="$(/usr/share/google/get_metadata_value attributes/init-actions-repo || echo ${DEFAULT_INIT_ACTIONS_REPO})"
# directory files ingestied will reside
readonly INIT_ACTIONS_DIR='/usr/lib/hive-llap'

function pre_flight_checks(){
    # check for bad configurations
    if [[ "${NUM_LLAP_NODES}" -ge "${WORKER_NODE_COUNT}" ]]; then
        echo "LLAP node count equals total worker count. There are no nodes to support Tez AM's. Please reduce LLAP instance count and re-deploy." && exit 1
    fi
}

# modify yarn-site.xml buy adjusting the classpath
function configure_yarn_site(){
    echo "configure yarn-site.xml..."

    local yarnappclasspath="$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.application.classpath)"

    # append new paths to the yarn.application.classpath
    bdconfig set_property \
        --configuration_file "/etc/hadoop/conf/yarn-site.xml" \
        --name "yarn.application.classpath" \
        --value "${yarnappclasspath},/usr/local/share/google/dataproc/lib/*,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/tez/*,/usr/lib/tez/lib/*" \
        --clobber

    # Ensure that the max container memory 
    if [[ "${NODE_MANAGER_MEMORY}" != "${YARN_MAX_CONTAINER_MEMORY}" ]]; then
       echo "not configured properly..."
    fi
}

function download_init_actions() {
    # Download initialization actions locally. This will download the start_llap.sh file to the cluster for execution Check if metadata is supplied
    echo "downalod init actions supplied as metadata..."
    mkdir -p "${INIT_ACTIONS_DIR}"
    gsutil cp "${INIT_ACTIONS_REPO}/hive-llap/start_llap.sh" "${INIT_ACTIONS_DIR}"
    chmod 700 "${INIT_ACTIONS_DIR}/start_llap.sh"
}

# add configurations to hive-site for LLAP
function configure_hive_site(){

    # different configuration if HA
    echo "configure hive-site.xml...."

    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.llap.daemon.service.hosts' --value '@llap0' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.server2.zookeeper.namespace' --value 'hiveserver2-interactive' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.execution.mode' --value 'llap' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.llap.execution.mode' --value 'only' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.llap.io.enabled' --value 'true' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.server2.enable.doAs' --value 'false' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.txn.manager' --value 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.support.concurrency' --value 'true' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.llap.io.allocator.alloc.min' --value '256Kb' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.compactor.initiator.on' --value 'true' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.compactor.worker.threads' --value '1' \
        --clobber
    bdconfig set_property \
        --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
        --name 'hive.tez.container.size' --value "${EXECUTOR_SIZE}" \
        --clobber

    if [[ -z "$ADDITIONAL_MASTER" ]]; then
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.zookeeper.quorum' --value "${LLAP_MASTER_FQDN}:2181" \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.zookeeper.client.port' --value '2181' \
            --clobber
    fi
}

# add configurations to core-site for LLAP; add zookeeper details
function configure_core_site(){
    echo "configure core-site.xml..."

    if [[ -n "$ADDITIONAL_MASTER" ]]; then
        bdconfig set_property \
            --configuration_file "${HADOOP_CONF_DIR}/core-site.xml" \
            --name 'hadoop.registry.zk.quorum' --value "\${hadoop.zk.address}" \
            --clobber
    else
        bdconfig set_property \
            --configuration_file "${HADOOP_CONF_DIR}/core-site.xml" \
            --name 'hadoop.registry.zk.quorum' --value "${LLAP_MASTER_FQDN}:2181" \
            --clobber
    fi

    bdconfig set_property \
        --configuration_file "${HADOOP_CONF_DIR}/core-site.xml" \
        --name 'hadoop.registry.zk.root' --value "/registry" \
        --clobber
    bdconfig set_property \
        --configuration_file "${HADOOP_CONF_DIR}/core-site.xml" \
        --name 'hadoop.registry.rm.enabled' --value "true" \
        --clobber
}

# add missing log4j file on all nodes
function get_log4j() {
    echo "import missing log4j library..."
    wget -nv --timeout=30 --tries=5 --retry-connrefused \
        https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/2.10.0/log4j-slf4j-impl-2.10.0.jar
    cp log4j-slf4j-impl-2.10.0.jar /usr/lib/hive/lib
}

# repackage tez_lib_uris and place on HDFS; only need to do this on one node. 
function package_tez_lib_uris(){
    echo "repackage tez lib uris..."
    cp /usr/lib/tez/lib/* /usr/lib/tez
    cp /usr/local/share/google/dataproc/lib/* /usr/lib/tez
    tar -czvf tez.tar.gz /usr/lib/tez
    runuser -l hdfs -c 'hdfs dfs -mkdir /tez'
    until `hdfs dfs -copyFromLocal tez.tar.gz /tez`; do echo "Retrying"; sleep 10; done
}

# reconfigure tez.lib.uris to point to hdfs rather than local filesytem; run on all nodes so we have compatiable config files
function configure_tez_site_xml() {
    echo "reconfigure tez-site.xml..."
    sed -i 's@file:/usr/lib/tez,file:/usr/lib/tez/lib,file:/usr/local/share/google/dataproc/lib@${fs.defaultFS}/tez/tez.tar.gz@g' /etc/tez/conf/tez-site.xml
}

# add yarn service directory for hive; run only on one node since this is a HDFS command
function add_yarn_service_dir(){
    echo "adding yarn service directory on the hive user..."
    runuser -l hdfs -c 'hdfs dfs -mkdir /user/hive/.yarn'
    runuser -l hdfs -c 'hdfs dfs -mkdir /user/hive/.yarn/package'
    runuser -l hdfs -c 'hdfs dfs -mkdir /user/hive/.yarn/package/LLAP'
    runuser -l hdfs -c 'hdfs dfs -chown hive:hive /user/hive/.yarn/package/LLAP'
}

# All nodes need to run this. The log files need to be instantiated and the versions of runLlapDaemon and package.py don't work OOTB with dataproc
function replace_core_llap_files() {
    echo "replacing llap files..."

    # open missing properties files
    cp /usr/lib/hive/conf/llap-cli-log4j2.properties.template /usr/lib/hive/conf/llap-cli-log4j2.properties
    cp /usr/lib/hive/conf/llap-daemon-log4j2.properties.template /usr/lib/hive/conf/llap-daemon-log4j2.properties

    # modify file runLlapDaemon.sh
    sed -i '78s/$/:`hadoop classpath`/' /usr/lib/hive/scripts/llap/bin/runLlapDaemon.sh
    
    # modify file package.py
    sed -i 's/print \"Cannot find input files\"/print(\"Cannot find input files\")/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"Cannot determine the container size\"/print(\"Cannot determine the container size\")/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Running as a child of LlapServiceDriver\" % (strftime(\"%H:%M:%S\", gmtime()))/print("%s Running as a child of LlapServiceDriver" % (strftime("%H:%M:%S", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Running after LlapServiceDriver\" % (strftime(\"%H:%M:%S\", gmtime()))/print(\"%s Running after LlapServiceDriver\" % (strftime(\"%H:%M:%S\", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Prepared the files\" % (strftime(\"%H:%M:%S\", gmtime()))/print(\"%s Prepared the files\" % (strftime(\"%H:%M:%S\", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Packaged the files\" % (strftime(\"%H:%M:%S\", gmtime()))/print(\"%s Packaged the files\" % (strftime(\"%H:%M:%S\", gmtime())))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/0700/0o700/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/print \"%s Prepared %s\/run.sh for running LLAP on YARN\" % (strftime(\"%H:%M:%S\", gmtime()), output)/print(\"%s Prepared %s\/run.sh for running LLAP on YARN\" % (strftime(\"%H:%M:%S\", gmtime()), output))/g' /usr/lib/hive/scripts/llap/yarn/package.py
    sed -i 's/long(max_direct_memory)/int(max_direct_memory)/g' /usr/lib/hive/scripts/llap/yarn/package.py
}

# if the user has added ssd=1 as a metadata option, then we need to configure hive-site.xml for using the ssd as a memory cache extension. The configuration must
# be done on all nodes BUT the actual permissions/ownership needs only to be done on the workers. 
function configure_SSD_caching_worker(){
    if [[ -n "$HAS_SSD" ]]; then
        echo "configure ssd hive-site params on workers"
        mkdir /mnt/1/llap
        chown hive:hadoop /mnt/1/llap
        chmod 770 /mnt/1/llap

        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.allocator.mmap' --value 'true' \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.allocator.mmap.path' --value '/mnt/1/llap' \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.memory.mode' --value 'cache' \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.use.lrfu' --value 'true' \
            --clobber
    fi
}
# if the user has added ssd=1 as a metadata option, then we need to configure hive-site.xml for using the ssd as a memory cache extension. 
function configure_SSD_caching_master(){
    if [[ -n "$HAS_SSD" ]]; then
        echo "configure ssd hive-site params on master"

        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.allocator.mmap' --value 'true' \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.allocator.mmap.path' --value '/mnt/1/llap' \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.memory.mode' --value 'cache' \
            --clobber
        bdconfig set_property \
            --configuration_file "${HIVE_CONF_DIR}/hive-site.xml" \
            --name 'hive.llap.io.use.lrfu' --value 'true' \
            --clobber
    fi
}

# start LLAP - Master Node
function start_llap(){
    if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then
        echo "starting llap on master node 0..."
        bash "${INIT_ACTIONS_DIR}"/start_llap.sh
    fi
}

# main driver function for the script
function configure_llap(){

    if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then
        echo "running primary master config...."
        pre_flight_checks
        download_init_actions
        package_tez_lib_uris
        add_yarn_service_dir
        configure_yarn_site
        configure_core_site
        configure_hive_site
        configure_SSD_caching_master
        replace_core_llap_files
        get_log4j
        configure_tez_site_xml

    elif [[ "${ROLE}" == "Worker" ]]; then
        echo "running worker config...."
        pre_flight_checks
        configure_yarn_site
        configure_core_site
        configure_hive_site
        configure_SSD_caching_worker
        get_log4j
        configure_tez_site_xml
        replace_core_llap_files

    elif [[ "${ROLE}" == "Master" && "${HOSTNAME}" != "${LLAP_MASTER_FQDN}"  ]]; then
        echo "running master config...."
        pre_flight_checks
        configure_yarn_site
        configure_core_site
        configure_hive_site
        get_log4j
        configure_tez_site_xml
        replace_core_llap_files
    fi
}

echo "Running configuration process...."
configure_llap

echo "Starting LLAP...."
start_llap

echo "LLAP Setup & Start Complete!"