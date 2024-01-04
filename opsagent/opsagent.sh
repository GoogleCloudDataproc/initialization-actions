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

# Detect dataproc image version from its various names
if (! test -v DATAPROC_IMAGE_VERSION) && test -v DATAPROC_VERSION; then
  DATAPROC_IMAGE_VERSION="${DATAPROC_VERSION}"
fi

if [[ $(echo "${DATAPROC_IMAGE_VERSION} < 2.2" | bc -l) == 1  ]]; then
  echo "This Dataproc cluster node runs image version ${DATAPROC_IMAGE_VERSION} with pre-installed legacy monitoring agent. Skipping Ops Agent installation."
  exit 0
fi

curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
bash add-google-cloud-ops-agent-repo.sh --also-install

cat <<EOF >> /etc/google-cloud-ops-agent/config.yaml
logging:
  service:
    pipelines:
      default_pipeline:
        receivers: []
EOF

systemctl restart google-cloud-ops-agent
