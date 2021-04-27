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

readonly LLAP_MASTER_FQDN="$(/usr/share/google/get_metadata_value attributes/dataproc-master)"
readonly WORKER_NODE_COUNT=$(/usr/share/google/get_metadata_value attributes/dataproc-worker-count)
readonly NODE_MANAGER_vCPU=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.nodemanager.resource.cpu-vcores"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly NODE_MANAGER_MEMORY=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.nodemanager.resource.memory-mb"]/value' -nl /etc/hadoop/conf/yarn-site.xml)
readonly YARN_MAX_CONTAINER_MEMORY=$(xmlstarlet sel -t -v '/configuration/property[name = "yarn.scheduler.maximum-allocation-mb"]/value' -nl /etc/hadoop/conf/yarn-site.xml)

##start LLAP - Master Node
function start_llap(){


	if [[ "${HOSTNAME}" == "${LLAP_MASTER_FQDN}" ]]; then

		echo "restart hive server prior..."
		sudo systemctl restart hive-server2.service 

		echo "starting yarn app fastlaunch...."
		yarn app -enableFastLaunch

		echo "Setting Parameters for LLAP start"
		
		LLAP_SIZE=$NODE_MANAGER_MEMORY
		echo "LLAP daemon size: $LLAP_SIZE"

		LLAP_CPU_ALLO=0
		LLAP_MEMORY_ALLO=0
		LLAP_XMX=0
		LLAP_EXECUTORS=0

		###Get the number of exeuctors based on memory
		for ((i = 1; i <= $NODE_MANAGER_vCPU; i++)); do
        LLAP_MEMORY_ALLO=$(($i * 4096))
        if (( $LLAP_MEMORY_ALLO < $(expr $NODE_MANAGER_MEMORY - 6114) )); then
                LLAP_EXECUTORS=$i
                LLAP_XMX=$LLAP_MEMORY_ALLO
        fi
		done

		echo "LLAP executors: ${LLAP_EXECUTORS}"
		echo "LLAP xmx memory: ${LLAP_XMX}"

		### 6% of xmx or max 6GB for jvm headroom
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

function wait_for_llap_ready() {

	echo "wait for LLAP to launch...."
	sudo -u hive hive --service llapstatus --name llap0 -w -r 1 -i 5
	echo "LLAP started...."
}

echo "Starting LLAP...."
start_llap

echo "Verify full start...."
wait_for_llap_ready

echo "LLAP Restart Complete!"
