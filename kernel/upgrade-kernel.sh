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
# This script installs the latest kernel and reboots

# Fail on error
set -euxo pipefail

OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')

# Determine which kernel is installed
if [[ "${OS_NAME}" == "debian" ]]; then
  CURRENT_KERNEL_VERSION=`cat /proc/version  | perl -ne 'print( / Debian (\S+) / )'`
elif [[ "${OS_NAME}" == "ubuntu" ]]; then
  CURRENT_KERNEL_VERSION=`cat /proc/version | perl -ne 'print( /^Linux version (\S+) / )'`
elif [[ ${OS_NAME} == rocky ]]; then
  KERN_VER=$(yum info --installed kernel | awk '/^Version/ {print $3}')
  KERN_REL=$(yum info --installed kernel | awk '/^Release/ {print $3}')
  CURRENT_KERNEL_VERSION="${KERN_VER}-${KERN_REL}"
else
  echo "unsupported OS: ${OS_NAME}!"
  exit -1
fi

# Get latest version available in repos
if [[ "${OS_NAME}" == "debian" ]]; then
  apt-get -qq update
  TARGET_VERSION=$(apt-cache show --no-all-versions linux-image-amd64 | awk '/^Version/ {print $2}')
elif [[ "${OS_NAME}" == "ubuntu" ]]; then
  apt-get -qq update
  LATEST_VERSION=$(apt-cache show --no-all-versions linux-image-gcp | awk '/^Version/ {print $2}')
  TARGET_VERSION=`echo ${LATEST_VERSION} | perl -ne 'printf(q{%s-%s-gcp},/(\d+\.\d+\.\d+)\.(\d+)/)'`
elif [[ "${OS_NAME}" == "rocky" ]]; then
  if yum info --available kernel ; then
    KERN_VER=$(yum info --available kernel | awk '/^Version/ {print $3}')
    KERN_REL=$(yum info --available kernel | awk '/^Release/ {print $3}')
    TARGET_VERSION="${KERN_VER}-${KERN_REL}"
  else
    TARGET_VERSION="${CURRENT_KERNEL_VERSION}"
  fi
fi

# Skip this script if we are already on the target version
if [[ "${CURRENT_KERNEL_VERSION}" == "${TARGET_VERSION}" ]]; then
  echo "target kernel version [${TARGET_VERSION}] is installed"
  exit 0
fi

# Install the latest kernel
if [[ ${OS_NAME} == debian ]]; then
  apt-get install -y linux-image-amd64
elif [[ "${OS_NAME}" == "ubuntu" ]]; then
  apt-get install -y linux-image-gcp
elif [[ "${OS_NAME}" == "rocky" ]]; then
  dnf -y -q install kernel
fi

# Make it possible to reboot before init actions are complete - #1033
DP_ROOT=/usr/local/share/google/dataproc
STARTUP_SCRIPT="${DP_ROOT}/startup-script.sh"
POST_HDFS_STARTUP_SCRIPT="${DP_ROOT}/post-hdfs-startup-script.sh"

for startup_script in ${STARTUP_SCRIPT} ${POST_HDFS_STARTUP_SCRIPT} ; do
  sed -i -e 's:/usr/bin/env bash:/usr/bin/env bash\nexit 0:' ${startup_script}
done

cp /var/log/dataproc-initialization-script-0.log /var/log/dataproc-initialization-script-0.log.0

systemctl reboot
