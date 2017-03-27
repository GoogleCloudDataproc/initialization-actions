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

# This script installs Hive HCatalog
# (https://cwiki.apache.org/confluence/display/Hive/HCatalog) on a Google Cloud
# Dataproc cluster.
#
# To use this script, you will need to configure the following variables to
# match your cluster. For information about which software components
# (and their version) are included in Cloud Dataproc clusters, see the
# Cloud Dataproc Image Version information:
# https://cloud.google.com/dataproc/concepts/dataproc-versions

set -x -e

# Install the hive-hcatalog package
apt-get -q -y install hive-hcatalog

# Configure Pig to use HCatalog
cat >>/etc/pig/conf/pig-env.sh <<EOF
#!/bin/bash

includeHCatalog=true

EOF
