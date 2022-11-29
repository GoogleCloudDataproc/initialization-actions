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
# This script reverts the kernel to a version which is capable of building older
# NVidia GPU kernel drivers, and then reboots into the new kernel

# Fail on error
set -euxo pipefail

UNAME_R=$(uname -r)
OS_NAME=$(lsb_release -is | tr '[:upper:]' '[:lower:]')

if [[ ${OS_NAME} == debian ]]; then
  TARGET_VERSION='5.7.0-3-amd64'
elif [[ ${OS_NAME} == rocky ]]; then
  TARGET_VERSION='4.18.0-305.10.2.el8_4.x86_64'
else
  exit 0
fi

if [[ "${UNAME_R}" == "${TARGET_VERSION}" ]]; then
  echo "target kernel version [${TARGET_VERSION}] is installed"
  exit 0
fi

function get_metadata_attribute() {
  local -r attribute_name=$1
  local -r default_value=$2
  /usr/share/google/get_metadata_value "attributes/${attribute_name}" || echo -n "${default_value}"
}

DRIVER_VERSION=$(get_metadata_attribute 'gpu-driver-version' "460.32.03")
CUDA_VERSION=$(get_metadata_attribute 'cuda-version' '11.2')

# Only run this script for rocky8 when targetting NVidia kernel driver 455 or earlier
if [[ ${OS_NAME} == debian ]]; then
  if [[ ${DRIVER_VERSION%%.*} > "450"
     && $(echo "${CUDA_VERSION} > 11.0" | bc -l) == 1 ]]; then exit 0; fi
  PREBUILD_KERNEL_PREFIX="https://web.c9h.org/~cjac/for-dataproc/2.0-debian10/kernel-5.7"
  PREBUILD_KERNEL_URL="${PREBUILD_KERNEL_PREFIX}/linux-image-5.7.0-3-amd64-unsigned_5.7.17-1_amd64.deb"
  pushd /usr/src
  if [[ "$(curl -s -I ${PREBUILD_KERNEL_URL} | head -1 | awk '{print $2}')" != "200" ]]; then
    # https://gist.github.com/malakudi/b90fe4c5b7ca6182fda07ed51e8eaaa5
    apt-get install -y -q rsync quilt kernel-wedge flex bison \
      libelf-dev dh-exec libpci-dev libudev-dev libcap-dev libiberty-dev \
      python3-distutils python3-dev asciidoctor libwrap0-dev
    git clone https://salsa.debian.org/kernel-team/linux.git
    wget 'https://mirrors.edge.kernel.org/pub/linux/kernel/v5.x/linux-5.7.17.tar.xz'
    cd linux
    git checkout debian/5.7.17-1
    sed -i -e 's/debug-info: true/debug-info: false/' debian/config/defines
    sed -i -e 's/gcc-9/gcc-8/g' \
      debian/config/defines \
      debian/config/amd64/defines \
      debian/templates/control.extra.in
    debian/bin/genorig.py ../linux-5.7.17.tar.xz
    debian/rules orig
    debian/rules debian/control || test -f debian/control
    fakeroot debian/rules source
    fakeroot make -j24 -f debian/rules.gen binary-arch_amd64_none_amd64
    #fakeroot make -j24 -f debian/rules.gen binary-arch_amd64_real
    fakeroot debian/rules binary-indep
    cd ..
  else
    
    for pkg in \
      linux-image-5.7.0-3-amd64-unsigned_5.7.17-1_amd64.deb \
      linux-headers-5.7.0-3-amd64_5.7.17-1_amd64.deb \
      linux-source-5.7_5.7.17-1_all.deb \
      linux-headers-5.7.0-3-common_5.7.17-1_all.deb \
      linux-kbuild-5.7_5.7.17-1_amd64.deb
    do
      wget "${PREBUILD_KERNEL_PREFIX}/${pkg}"
    done
  fi

  # don't tell us we're uninstalling the current kernel
  mv /usr/bin/linux-check-removal /usr/bin/linux-check-removal.orig
  echo -e '#!/bin/sh\nexit 0' | tee /usr/bin/linux-check-removal
  chmod +x /usr/bin/linux-check-removal
  apt-get -y purge $(dpkg -l | awk '/linux-image/ {print $2}')
  mv /usr/bin/linux-check-removal.orig /usr/bin/linux-check-removal

  sudo dpkg -i *.deb
  popd
elif [[ ${OS_NAME} == rocky ]]; then
  if [[ ${DRIVER_VERSION%%.*} > "455" 
     && $(echo "${CUDA_VERSION} > 11.1" | bc -l) == 1 ]]; then exit 0; fi

  cd /tmp

  function execute_with_retries() {
    local -r cmd=$1
    for ((i = 0; i < 10; i++)); do
      if eval "$cmd"; then
        return 0
      fi
      sleep 5
    done
    return 1
  }

  URL_PFX="http://mirror.centos.org/centos/8-stream/BaseOS/x86_64/os/Packages"
  PKGS="kernel kernel-core kernel-modules kernel-headers kernel-tools kernel-tools-libs kernel-devel"

  for pkg in ${PKGS}
  do
    wget -q ${URL_PFX}/${pkg}-${TARGET_VERSION}.rpm
  done

  execute_with_retries "dnf -y -q update"
  execute_with_retries "dnf install -y kernel-*${TARGET_VERSION}.rpm"

  # pin the kernel packages
  dnf install -y 'dnf-command(versionlock)'
  for pkg in ${PKGS}
  do
    dnf versionlock ${pkg}-${TARGET_VERSION}
  done
fi

# Make it possible to reboot before init actions are complete - #1033
DP_ROOT=/usr/local/share/google/dataproc
STARTUP_SCRIPT="${DP_ROOT}/startup-script.sh"
POST_HDFS_STARTUP_SCRIPT="${DP_ROOT}/post-hdfs-startup-script.sh"

for startup_script in ${STARTUP_SCRIPT} ${POST_HDFS_STARTUP_SCRIPT} ; do
  sed -i -e 's:/usr/bin/env bash:/usr/bin/env bash\nexit 0:' ${startup_script}
done

# DP_ROOT=/usr/local/share/google/dataproc
# STARTUP_SCRIPT="${DP_ROOT}/startup-script.sh"

# sed -i -e 's:rm /etc/udev/rules.d/80-ttyS3.rules:rm -f /etc/udev/rules.d/80-ttyS3.rules:' ${DP_ROOT}/bdutil/configure_keys.sh
# sed -i -e 's:ln -s ":ln -sf ":g' ${STARTUP_SCRIPT}
# sed -i -e 's:function create_docker_wrapper() {:function create_docker_wrapper() {\n  if [[ -f "/usr/bin/docker.real" ]]; then return 0 ; fi:' ${DP_ROOT}/bdutil/components/activate/docker-ce.sh
# sed -i -e 's:^activate_hdfs$:if [[ ! -d /var/run/hadoop-hdfs || $(ls /var/run/hadoop-hdfs/*.pid | wc -l) -eq 0 ]]; then activate_hdfs ; fi:' ${DP_ROOT}/bdutil/components/activate/hdfs.sh
# sed -i -e 's:"${MY_HOSTNAME}" != "${DATAPROC_MASTER}":"${MY_HOSTNAME}" != "${DATAPROC_MASTER}" || $(systemctl is-enabled mysqld) == "enabled":' ${DP_ROOT}/bdutil/components/pre-activate/mysql.sh

# sed -i -e 's:/usr/bin/env bash:/usr/bin/env bash\nexit 0:' $STARTUP_SCRIPT

cp /var/log/dataproc-initialization-script-0.log /var/log/dataproc-initialization-script-0.log.0

systemctl reboot
