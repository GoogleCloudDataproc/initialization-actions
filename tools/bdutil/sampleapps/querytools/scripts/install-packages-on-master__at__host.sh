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

readonly SCRIPTDIR=$(dirname $0)

# Pull in global properties
source project_properties.sh

# Pull in common functions
source $SCRIPTDIR/common_utils.sh

# Files to push to master; place project_properties.sh in the same directory
# as the other scripts
readonly SCRIPT_FILES_TO_PUSH="\
  project_properties.sh \
  $SCRIPTS_DIR/common_utils.sh \
  $SCRIPTS_DIR/package_utils.sh \
  $SCRIPTS_DIR/setup-hdfs-for-hdtools__at__master.sh \
  $SCRIPTS_DIR/setup-packages__at__master.sh \
  $SCRIPTS_DIR/setup-ssh-keys__at__master.sh \
"
readonly MASTER_PACKAGE_SUBDIRS="\
  $MASTER_PACKAGE_DIR/$SCRIPTS_DIR \
  $MASTER_PACKAGE_DIR/conf/hive \
  $MASTER_PACKAGE_DIR/ssh-key
"

# Ensure permissions on the script files before we push them
chmod 755 $SCRIPT_FILES_TO_PUSH

# Create the destination directory on the master
emit ""
emit "Ensuring setup directories exist on master:"
gcutil ssh --zone=$ZONE --ssh_arg -t $MASTER sudo -i \
        "rm -rf $MASTER_PACKAGE_DIR && \
         mkdir -p $MASTER_PACKAGE_SUBDIRS"

# Push the setup script to the master
emit ""
emit "Pushing the setup scripts to the master:"
gcutil push --zone=$ZONE $MASTER \
        $SCRIPT_FILES_TO_PUSH $MASTER_PACKAGE_DIR/$SCRIPTS_DIR

# Push configuration to the master
emit ""
emit "Pushing configuration to the master:"
gcutil push --zone=$ZONE $MASTER \
        conf/hive/* $MASTER_PACKAGE_DIR/conf/hive

# Execute the setup script on the master
emit ""
emit "Launching the user and package setup script on the master:"
gcutil ssh --zone=$ZONE --ssh_arg -t $MASTER \
        sudo $MASTER_PACKAGE_DIR/$SCRIPTS_DIR/setup-packages__at__master.sh

# Execute the HDFS setup script on the master
emit ""
emit "Launching the HDFS setup script on the master:"
gcutil ssh --zone=$ZONE --ssh_arg -t $MASTER \
        sudo \
        $MASTER_PACKAGE_DIR/$SCRIPTS_DIR/setup-hdfs-for-hdtools__at__master.sh

# Set up SSH keys for the user
emit ""
emit "Generating SSH keys for user $HDP_USER"

readonly KEY_DIR=./ssh-key
mkdir -p $KEY_DIR
rm -f $KEY_DIR/$HDP_USER $KEY_DIR/${HDP_USER}.pub

ssh-keygen -t rsa -P '' -f $KEY_DIR/$HDP_USER
chmod o+r $KEY_DIR/${HDP_USER}.pub
emit "Pushing SSH keys for user $HDP_USER to $MASTER"
gcutil push --zone=$ZONE $MASTER \
        $KEY_DIR/${HDP_USER}.pub $MASTER_PACKAGE_DIR/ssh-key/
emit "Adding SSH public key for user $HDP_USER to authorized_keys"
gcutil ssh --zone=$ZONE --ssh_arg -t $MASTER \
        sudo sudo -u $HDP_USER -i \
        $MASTER_PACKAGE_DIR/$SCRIPTS_DIR/setup-ssh-keys__at__master.sh \
          $MASTER_PACKAGE_DIR/ssh-key

MASTER_IP=$(gcutil getinstance --zone=$ZONE $MASTER | \
            awk -F '|' \
              '$2 ~ / *external-ip */ { gsub(/[ ]*/, "", $3); print $3 }')

emit ""
emit "***"
emit "SSH keys generated locally to:"
emit "  Public key: $KEY_DIR/$HDP_USER.pub"
emit "  Private key: $KEY_DIR/$HDP_USER"
emit ""
emit "Public key installed on $MASTER to ~$HDP_USER/.ssh/authorized_keys"
emit ""
emit "You may now ssh to user $HDP_USER@$MASTER with:"
emit "   ssh -i $KEY_DIR/$HDP_USER $HDP_USER@$MASTER_IP"
emit "***"

emit ""
emit "Installation complete"
