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

# Adds a new 'hadoop' user which will be used to run the hadoop servers.

set -e

mkdir -p /home/hadoop
mkdir -p /home/hadoop/.ssh

if ! (id -u hadoop >& /dev/null); then
  useradd --system --shell /bin/bash -M --home /home/hadoop --user-group hadoop
fi

if skeleton_files=$(find /etc/skel/ -maxdepth 1 -type f); then
  cp ${skeleton_files} /home/hadoop
fi

chown -R hadoop:hadoop /home/hadoop

mkdir -p ~hadoop/.ssh
chown -R hadoop:hadoop ~hadoop/.ssh/

if [[ -x $(which restorecon) ]]; then
  restorecon -Rv /home
fi

mkdir -p /var/log/hadoop
chown hadoop:hadoop /var/log/hadoop
