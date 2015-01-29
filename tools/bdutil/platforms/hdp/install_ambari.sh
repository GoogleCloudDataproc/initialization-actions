#######################################################################

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
#######################################################################

## Name:    install_ambari.sh
## Purpose:
##   - Handle prerequisites for installation of Apache Ambari
##   - Install ambari-agent and ambari-server
##   - Configure ambari-server
# Usage:   Called from 'bdutil'. Do not run directly
#######################################################################

## disable selinux
setenforce 0
sed -i 's/\(^[^#]*\)SELINUX=enforcing/\1SELINUX=disabled/' /etc/selinux/config
sed -i 's/\(^[^#]*\)SELINUX=permissive/\1SELINUX=disabled/' /etc/selinux/config

## disable transparent_hugepages
cp -a ./thp-disable.sh /usr/local/sbin/
sh /usr/local/sbin/thp-disable.sh || /bin/true
echo -e '\nsh /usr/local/sbin/thp-disable.sh || /bin/true' >> /etc/rc.local

## disable iptables
chkconfig iptables off
service iptables stop

## swappiness to 0
sysctl -w vm.swappiness=0
cat > /etc/sysctl.d/50-swappiness.conf <<-'EOF'
## no more swapping
vm.swappiness=0
EOF

## install & start ntpd
yum install ntp -y
service ntpd start

# install Apache Ambari YUM repository
curl -Ls -o /etc/yum.repos.d/ambari.repo ${AMBARI_REPO}

# install Apache Ambari-agent
yum install ambari-agent -y
sed -i.orig "s/^.*hostname=localhost/hostname=${MASTER_HOSTNAME}/" \
    /etc/ambari-agent/conf/ambari-agent.ini

# script which detects the public IP of nodes in the cluster
#   disabled by default. To enable: set 'AMBARI_PUBLIC' to true in ambari_config.sh
cp -a ./public-hostname-gcloud.sh /etc/ambari-agent/conf/
if [ "${AMBARI_PUBLIC}" -eq 1 ]; then
    sed -i "/\[agent\]/ a public_hostname_script=\/etc\/ambari-agent\/conf\/public-hostname-gcloud.sh" /etc/ambari-agent/conf/ambari-agent.ini
else
    sed -i "/\[agent\]/ a #public_hostname_script=\/etc\/ambari-agent\/conf\/public-hostname-gcloud.sh" /etc/ambari-agent/conf/ambari-agent.ini
fi

# start Apache ambari-agent
service ambari-agent restart
chkconfig ambari-agent on

# install, configure and start Apache ambari-server on the master node
if [ "$(hostname)" = "${MASTER_HOSTNAME}" ]; then
  yum install -y ambari-server
  service ambari-server stop
  ambari-server setup -j ${JAVA_HOME} -s
  if ! nohup bash -c "service ambari-server start 2>&1 > /dev/null"; then
    echo 'Ambari Server failed to start' >&2
    exit 1
  fi
  chkconfig ambari-server on
fi
