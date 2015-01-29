# Copyright 2014 Google Inc. All Rights Reserved.
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

# Makes post-cluster build configuration changes

if (( ${INSTALL_GCS_CONNECTOR} )) ; then
    loginfo "adding /usr/local/lib/hadoop/lib to mapreduce.application.classpath."
    NEW_CLASSPATH=$(/var/lib/ambari-server/resources/scripts/configs.sh get localhost ${PREFIX} mapred-site | grep -E '^"mapreduce.application.classpath"' | tr -d \" | awk '{print "/usr/local/lib/hadoop/lib/*,"$3}' | sed 's/,$//')
    /var/lib/ambari-server/resources/scripts/configs.sh set localhost ${PREFIX} mapred-site mapreduce.application.classpath ${NEW_CLASSPATH}
    sleep 10

    loginfo "restarting services for classpath change to take affect."
    for SERVICE in YARN MAPREDUCE2; do
        ambari_service_stop
        ambari_wait_requests_completed
        ambari_service_start
        ambari_wait_requests_completed
    done
fi
