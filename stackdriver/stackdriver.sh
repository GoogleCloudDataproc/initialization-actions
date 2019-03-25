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
# This script installs the Stackdriver agent on your Cloud Dataproc
# cluster so you can monitor your cluster with Stackdriver.
#
# Once you create a cluster with this initialization action, you will
# need to perform some configuration in Stackdriver. Specifically, you must
# create a group based on the cluster name prefix. This will detect any new
# instances created with that prefix and use this group as the basis for
# your alerting policies and dashboards

# Download and run the Stackdriver installation script
curl -sSO https://dl.google.com/cloudagents/install-monitoring-agent.sh
sudo bash install-monitoring-agent.sh
