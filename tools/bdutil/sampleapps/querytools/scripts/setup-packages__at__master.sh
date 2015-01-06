#!/bin/bash
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o nounset
set -o errexit

SCRIPT=$(basename $0)
SCRIPTDIR=$(dirname $0)

source $SCRIPTDIR/project_properties.sh
source $SCRIPTDIR/common_utils.sh
source $SCRIPTDIR/package_utils.sh

# BEGIN: Package-specific setup functions

function setup_pkg_generic() {
  local pkg_dir="$1"
  local pkg_name="$2"
  local pkg_file="$3"
  local install_dir="$4"

  emit "Installing $pkg_name from $pkg_dir/$pkg_name/$pkg_file"

  local target_dir=${pkg_file%.tar.gz}
  emit "  Exploding $pkg_file into $install_dir"
  emit "  Logging to $pkg_dir/${target_dir}.log"
  if ! tar xvfz $pkg_dir/$pkg_name/$pkg_file \
             -C $install_dir > $MASTER_PACKAGE_DIR/${target_dir}.log; then
    exit 1
  fi

  emit "  Creating soft link '$pkg_name' to $target_dir"
  (cd $install_dir; ln -f -s $target_dir $pkg_name)
}
readonly -f setup_pkg_generic

function setup_pkg_hive() {
  setup_pkg_generic $@

  # Move configuration
  emit ""
  emit "Move hive configuration"
  mv $MASTER_PACKAGE_DIR/conf/hive/* $MASTER_INSTALL_DIR/hive/conf
  chown $HDP_USER:$HADOOP_GROUP -R $MASTER_INSTALL_DIR/hive/conf

  # Increase the heapsize when using the GCS connector
  if $($HADOOP_HOME/bin/hadoop org.apache.hadoop.conf.Configuration \
        | grep "<name>fs.gs.impl</name>" &> /dev/null); then
    emit "Detected use of GCS connector- increasing Hive heap size"

    echo "export HADOOP_HEAPSIZE=1024" >> $MASTER_INSTALL_DIR/hive/conf/hive-env.sh
  fi
}
readonly -f setup_pkg_hive

function setup_pkg_pig() {
  setup_pkg_generic $@
}
readonly -f setup_pkg_pig

# END: Package-specific setup functions

emit ""
emit "*** Begin: $SCRIPT running on master $(hostname) ***"

# Set up a "hadoop user" and add to the hadoop group
emit ""
emit "Checking for $HDP_USER"
if $(id -u $HDP_USER &> /dev/null); then
  emit "$HDP_USER already exists"
  usermod --gid $HADOOP_GROUP $HDP_USER
else
  emit "Creating user $HDP_USER in group $HADOOP_GROUP"
  useradd --gid $HADOOP_GROUP --shell /bin/bash -m $HDP_USER
fi

# Set up login environment
# Source our own file from the .profile so that we can overwrite with impunity
if ! test -e $HDP_USER_HOME/.profile || \
   ! grep --silent profile_hdtools $HDP_USER_HOME/.profile; then
  emit "Setting up $HDP_USER_HOME/.profile"
  echo "" >> $HDP_USER_HOME/.profile
  echo "# Pull in hadoop tool setup" >> $HDP_USER_HOME/.profile
  echo "source .profile_hdtools" >> $HDP_USER_HOME/.profile
fi

# Set common environment variables
emit "Setting up $HDP_USER_HOME/.profile_hdtools"
echo "" >| $HDP_USER_HOME/.profile_hdtools
JAVA_HOME=$(which java | xargs readlink -f | sed -E "s/\/(jre|jdk).*\/bin\/java$//")
echo "export JAVA_HOME=$JAVA_HOME" >> $HDP_USER_HOME/.profile_hdtools
echo "export HADOOP_PREFIX=$HADOOP_HOME" >> $HDP_USER_HOME/.profile_hdtools
echo "export PATH=\$HADOOP_PREFIX/bin:\"\$PATH\"" >> $HDP_USER_HOME/.profile_hdtools

# Pull down packages and unzip them into the install directory.
#  (rather than say /usr/local).
emit ""
emit "Copying package files from cloud storage"
for tool in $SUPPORTED_HDPTOOLS; do
  tool_uri=$(echo ${tool}_TARBALL_URI | tr '[:lower:]' '[:upper:'])

  mkdir -p $MASTER_PACKAGE_DIR/$PACKAGES_DIR/${tool}
  gsutil -q cp ${!tool_uri} $MASTER_PACKAGE_DIR/$PACKAGES_DIR/${tool}
done

PACKAGE_LIST=$(pkgutil_get_list $MASTER_PACKAGE_DIR/$PACKAGES_DIR)
if [[ -z $PACKAGE_LIST ]]; then
  die "No package found in $MASTER_PACKAGE_DIR/$PACKAGES_DIR"
fi

pkgutil_emit_list "$MASTER_PACKAGE_DIR/$PACKAGES_DIR" "$PACKAGE_LIST"

# Call package-specific functions to do the install
emit ""
emit "Installing packages into install directory $MASTER_INSTALL_DIR"

for pkg in $PACKAGE_LIST; do
  # Get the query-tool specific directory name
  pkg_name=$(pkgutil_pkg_name $MASTER_PACKAGE_DIR/$PACKAGES_DIR $pkg)
  pkg_upper=$(echo "$pkg_name" | tr '[a-z]' '[A-Z]')

  # Get the name of the zip file
  pkg_file=$(pkgutil_pkg_file $MASTER_PACKAGE_DIR/$PACKAGES_DIR $pkg)

  # Unzip the package file(s)
  setup_pkg_${pkg_name} $MASTER_PACKAGE_DIR/$PACKAGES_DIR \
        $pkg_name $pkg_file $MASTER_INSTALL_DIR

  # For each packages set <PKG>_HOME=/home/<user>/<pkg> into .profile
  echo "export ${pkg_upper}_HOME=\$HOME/$pkg_name" >> $HDP_USER_HOME/.profile_hdtools
  echo "export PATH=\$${pkg_upper}_HOME/bin:\"\$PATH\"" >> $HDP_USER_HOME/.profile_hdtools
done

emit "Setting user:group ownership on $MASTER_INSTALL_DIR to $HDP_USER:$HADOOP_GROUP"
chown -R $HDP_USER:$HADOOP_GROUP $MASTER_INSTALL_DIR

# Set group write permissions on the /hadoop/tmp directory.
# Depending on the version of the Hadoop solution, this may already be done.
emit "Setting group write permissions on hadoop temporary directory"

mkdir -p $HADOOP_TMP_DIR
chown $HADOOP_USER:$HADOOP_GROUP $HADOOP_TMP_DIR
chmod g+w $HADOOP_TMP_DIR

emit ""
emit "*** End: $SCRIPT running on master $(hostname) ***"
emit ""

