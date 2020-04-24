#!/usr/bin/env bash
# There is no pre-build tarball available if not using Dataproc 1.5+
# Following commands can be used to build Atlas 1.2.0 from source

# install maven 3.6
wget https://archive.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz -P /opt
tar -xvzf /opt/apache-maven-3.6.0-bin.tar.gz
mv apache-maven-3.6.0 /opt
echo "M2_HOME=/opt/apache-maven-3.6.0" >> /etc/environment
export PATH=$PATH:/opt/apache-maven-3.6.0/bin
sudo update-alternatives --install "/usr/bin/mvn" "mvn" "/opt/apache-maven-3.6.0/bin/mvn" 0
sudo update-alternatives --set mvn /opt/apache-maven-3.6.0/bin/mvn


git clone https://github.com/apache/atlas.git
cd atlas
git checkout tags/release-1.2.0-rc3
export MAVEN_OPTS="-Xms2g -Xmx2g"
mvn clean -DskipTests install
mvn clean -DskipTests package -Pdist
