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

# This script runs on the Hadoop master node as the target user ($HDP_USER).
# It is asssumed that a public key file for the user has been pushed
# onto the master node and the location of that file is the first argument
# to the script.

set -o nounset
set -o errexit

readonly SCRIPT=$(basename $0)
readonly SCRIPTDIR=$(dirname $0)

# Pull in global properties
source $SCRIPTDIR/project_properties.sh
source $SCRIPTDIR/common_utils.sh

if [[ $# -lt 1 ]]; then
  die "usage: $0 [keys-dir]"
fi

KEY_DIR=$1; shift
KEY_FILE=$KEY_DIR/${USER}.pub

if [[ ! -e $KEY_FILE ]]; then
  die "Public key file not found: $KEY_FILE"
fi

# Ensure that the .ssh directory and authorized_keys files exist
if [[ ! -e $HOME/.ssh/authorized_keys ]]; then
  mkdir -p $HOME/.ssh
  chmod 700 $HOME/.ssh

  touch $HOME/.ssh/authorized_keys
  chmod 600 $HOME/.ssh/authorized_keys
fi

# Add the public key file for the user to authorized_keys
emit "Updating $HOME/.ssh/authorized_keys"
(echo "# Added $(date)" && cat $KEY_FILE) >> $HOME/.ssh/authorized_keys

