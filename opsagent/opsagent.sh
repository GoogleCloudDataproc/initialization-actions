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

# This script installs the Google Cloud Ops Agent on each node in the cluster.
# It also provides an override to the built-in logging config to set empty
# receivers i.e. not collect any logs.
# If you need to collect syslogs, you can comment out editing user configuration file
# /etc/google-cloud-ops-agent/config.yaml below.
# See https://cloud.google.com/stackdriver/docs/solutions/agents/ops-agent/configuration#default
# for built-in configuration of Ops Agent.

curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash add-google-cloud-ops-agent-repo.sh --also-install

cat <<EOF >> /etc/google-cloud-ops-agent/config.yaml
logging:
  service:
    pipelines:
      default_pipeline:
        receivers: []
EOF

service google-cloud-ops-agent restart
