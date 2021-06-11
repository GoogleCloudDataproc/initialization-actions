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


# This script is for users that want to use the start/stop functionality with Dataproc. This
# script will automatically restart LLAP with the same configuration as previously. This script must be 
# run on the primary master node (0)


set -euxo pipefail

readonly LLAP_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_NODE_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly NODE_MANAGER_vCPU=$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.nodemanager.resource.cpu-vcores)
readonly NODE_MANAGER_MEMORY=$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.nodemanager.resource.memory-mb)
readonly YARN_MAX_CONTAINER_MEMORY=$(bdconfig get_property_value --configuration_file='/etc/hadoop/conf/yarn-site.xml' --name yarn.scheduler.maximum-allocation-mb)
readonly NUM_LLAP_NODES=$(/usr/share/google/get_metadata_value attributes/num-llap-nodes)
readonly EXECUTOR_SIZE=$(/usr/share/google/get_metadata_value attributes/exec_size_mb || echo 4096)

# start LLAP - Master Node
function start_llap(){
    
    if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then

        local llap_memory_allo=0
        local llap_xmx=0
        local llap_executors=0
        local llap_headroom=0
        local llap_instances=0

        echo "restart hive server prior..."
        sudo systemctl restart hive-server2.service 

        echo "starting yarn app fastlaunch...."
        yarn app -enableFastLaunch

        echo "Setting Parameters for LLAP start"
        
        local llap_size=$NODE_MANAGER_MEMORY
        echo "LLAP daemon size: $llap_size"

        # Get the number of exeuctors based on memory
        for ((i = 1; i <= $NODE_MANAGER_vCPU; i++)); do
            llap_memory_allo=$(($i * ${EXECUTOR_SIZE}))
            if (( $llap_memory_allo < $(expr $NODE_MANAGER_MEMORY - 6114) )); then
                llap_executors=$i
                llap_xmx=$llap_memory_allo
            fi
        done

        echo "LLAP executors: ${llap_executors}"
        echo "LLAP xmx memory: ${llap_xmx}"

        # 6% of xmx or max 6GB for jvm headroom
        local llap_xmx_6=$(echo "scale=0;${llap_xmx}*.06" |bc)
        local llap_xmx_6_int=${llap_xmx_6%.*}

        # jvm headroom for the llap executors
        if  (( $llap_xmx_6_int > 6144 )); then
            llap_headroom=6114
        else
            llap_headroom=$llap_xmx_6_int
        fi
        echo "LLAP daemon headroom: ${llap_headroom}"

        # cache is whatever is left over after heardroom and executor memory is accounted for
        local llap_cache=$(expr ${llap_size} - ${llap_headroom} - ${llap_xmx})


        # if there is no additional room, then no cache will be used
        if (( $llap_cache < 0 )); then
             llap_cache=0
        fi
        echo "LLAP in-memory cache: ${llap_cache}"

        # keep one node in reserve for handling the duties of Tez AM
        # if user didn't pass in num llap instances, take worker node count -1
        if [[ -z $NUM_LLAP_NODES ]]; then
            llap_instances=$(expr ${WORKER_NODE_COUNT} - 1) 
        else
            llap_instances=$NUM_LLAP_NODES
        fi 

        echo "LLAP daemon instances: ${llap_instances}"

        echo "Starting LLAP..."
        sudo -u hive hive --service llap \
            --instances "${llap_instances}" \
            --size "${llap_size}"m \
            --executors "${llap_executors}" \
            --xmx "${llap_xmx}"m \
            --cache "${llap_cache}"m \
            --name llap0 \
            --auxhbase=false \
            --directory /tmp/llap_staging \
            --output /tmp/llap_output \
            --loglevel INFO \
            --startImmediately
    fi
}

function wait_for_llap_ready() {

    echo "wait for LLAP to launch...."
    sudo -u hive hive --service llapstatus --name llap0 -w -r 1 -i 5    
    echo "LLAP started...."
}

echo "Starting LLAP...."
start_llap

echo "Verify full start...."
wait_for_llap_ready

echo "LLAP Start Complete!"
