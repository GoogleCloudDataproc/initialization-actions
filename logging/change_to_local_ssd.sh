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
# This script sets the yarn.nodemanager.log-dirs value to the Local SSD mounts.

# Dataproc configurations
# Modify these configurations as needed
# MAX_MNT_DISK_FOR_LOGS is the maximum number of disks to be used for creating the logs
# LOGPATH is the path under /mnt/<mount number> where the application logs are created
readonly MAX_MNT_DISK_FOR_LOGS=3
readonly LOGPATH='hadoop/yarn/userlogs'

# DO NOT CHANGE THESE VARIABLES UNLESS THE SOURCE FRAMEWORKS MODIFY IT
readonly FLUENTD_YARN_USERLOGS='/etc/google-fluentd/config.d/dataproc-yarn-userlogs.conf'
readonly HADOOP_CONF_DIR='/etc/hadoop/conf'

function set_hadoop_property() {
    local -r config_file=$1
    local -r property=$2
    local -r value=$3
    bdconfig set_property \
        --configuration_file "${HADOOP_CONF_DIR}/${config_file}" \
        --name "${property}" --value "${value}" \
        --clobber
}

# This function validates the MAX_MNT_DISK_FOR_LOGS variable and ensures that the value is capped at the max disks available
function get_max_local_ssd() {
    max_disk_no=0
    for line in $(df -h --output=target); do
        if [ ${line:0:4} = '/mnt' ]; then
            disk_no=$(echo $line | cut -d "/" -f3)
            if [ $disk_no ] >$max_disk_no; then
                max_disk_no=${disk_no}
            fi
        fi
    done
    echo $((max_disk_no > MAX_MNT_DISK_FOR_LOGS ? MAX_MNT_DISK_FOR_LOGS : max_disk_no)) # Return the least of total disks and maximum disks to be used for logging
}

# This function constructs the log directory paths
function construct_log_dirs() {
    max_disk_no=$(get_max_local_ssd)
    #echo "Max Disk = "$max_disk_no
    disk_no=1
    log_dirs=""
    while [ $disk_no -le $max_disk_no ]; do
        log_dirs+="/mnt/"$disk_no"/"$LOGPATH","
        disk_no=$((${disk_no} + 1))
    done
    echo $log_dirs | sed 's/,$//g'
}

function configure_yarn() {
    ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
    echo "Role = "$ROLE
    logdirs=$(construct_log_dirs)
    echo "Log dir = " $logdirs
    if [[ "${ROLE}" != 'Master' && ! -z $logdirs ]]; then
        # This block is entered only if the script is running on a Worker node and the logdirs variable is not empty which means
        # that the worker has local SSDs. If not, the property is left at default
        echo "Setting properties"
        set_hadoop_property 'yarn-site.xml' 'yarn.nodemanager.log-dirs' $logdirs
        echo "Complete"
    fi
}

#Getting Container Logs of Executor from mount paths added in yarn.nodemanager.log-dirs
# This function adds the /mnt/ paths to the fluentd configuration file to the path variable
function configure_fluentd() {
  sudo sed -i "s:path:path /mnt/\*/$LOGPATH/*,:g" $FLUENTD_YARN_USERLOGS
}


function main() {
    configure_yarn
    # Adding new log paths to configure_fluentd
    configure_fluentd
    # Restart YARN services if they are running already
    if [[ $(systemctl show hadoop-yarn-resourcemanager.service -p SubState --value) == 'running' ]]; then
        systemctl restart hadoop-yarn-resourcemanager.service
    fi
    if [[ $(systemctl show hadoop-yarn-nodemanager.service -p SubState --value) == 'running' ]]; then
        systemctl restart hadoop-yarn-nodemanager.service
    fi
    #Restart Fluentd services to reflect the new changes if they are running already
    if [[ $(systemctl show google-fluentd.service -p SubState --value) == 'running' ]]; then
        systemctl restart google-fluentd.service
    fi
}

main
