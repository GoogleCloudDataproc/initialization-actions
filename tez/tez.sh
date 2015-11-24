#!/bin/bash
#  Copyright 2015 Google, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.set -x -e
set -x -e

#  Define important variables
tez_version="0.7.0"
protobuf_version="2.5.0"
tez_hdfs_path="/apps"
tez_jars="/usr/lib/tez"
tez_conf_dir="/etc/tez"
role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"

if [[ "${role}" == 'Master' ]]; then
  #  Install needed items from apt-get
  apt-get install maven autoconf automake libtool curl nodejs npm -y
  
  #  Install required protobuf libs
  wget https://github.com/google/protobuf/releases/download/v${protobuf_version}/protobuf-${protobuf_version}.tar.gz
  tar -zxvf protobuf-${protobuf_version}.tar.gz
  cd protobuf-${protobuf_version}/
  ./autogen.sh
  ./configure
  make
  make check
  make install
  ldconfig
  
  #  Get Tez
  wget http://www.eu.apache.org/dist/tez/${tez_version}/apache-tez-${tez_version}-src.tar.gz
  tar -zxvf apache-tez-${tez_version}-src.tar.gz
  cd apache-tez-${tez_version}-src
  
  #  Configure Tez to work as root
  (
  cd tez-ui || exit
  LINE="$(grep -n "remove-unnecessary-resolutions=false" pom.xml | cut -d: -f1)"
  sed -i "${LINE}i<argument>--allow-root</argument>" pom.xml
  )
  
  #  Build Tez
  mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true
  
  #  Stage Tez
  hadoop fs -mkdir ${tez_hdfs_path}
  hadoop fs -copyFromLocal tez-dist/target/tez-${tez_version}.tar.gz ${tez_hdfs_path}/
  
  #  Write Tez config
  mkdir ${tez_conf_dir}
  cat > ${tez_conf_dir}/tez-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
   <name>tez.lib.uris</name>
   <value>\${fs.defaultFS}${tez_hdfs_path}/tez-${tez_version}.tar.gz</value>
  </property>
</configuration>
EOF

  #  Move Tez jars
  mkdir ${tez_jars}
  tar -zxvf tez-dist/target/tez-${tez_version}-minimal.tar.gz -C $tez_jars
  
  #  Update the hadoop-env.sh
  {
    echo "export TEZ_CONF_DIR=/etc/tez/"
    echo "export tez_jars=${tez_jars}"
    echo "HADOOP_CLASSPATH=\$HADOOP_CLASSPATH:${tez_conf_dir}:${tez_jars}/*:${tez_jars}/lib/*"
  } >> /etc/hadoop/conf/hadoop-env.sh
fi

set +x +e