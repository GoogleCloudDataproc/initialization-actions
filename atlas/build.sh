#!/usr/bin/env bash
# There is no pre-build tarball available.
# Following commands can be used to build Atlas 1.1.0 from sources, modified with two commits:
# - fixing build due to findbugs problem
# - better logs
#

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
git checkout tags/release-1.1.0-rc2
export MAVEN_OPTS="-Xms2g -Xmx2g"
git cherry-pick 6435f75db312e2944e8eaf72537e87491cd7af1a # for findbugs
git cherry-pick f5fd57475d8aec1a41d694f9f8bd6337ab9a8560 # for better logs

export MAVEN_OPTS="-Xms2g -Xmx2g"
mvn clean -DskipTests install
mvn clean -DskipTests package -Pdist
