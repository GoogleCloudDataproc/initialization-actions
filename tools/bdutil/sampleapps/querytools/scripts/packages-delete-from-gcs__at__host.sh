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

# packages-delete-from-gcs
#   This script removes the Hadoop query tool packages from Google Cloud
#   Storage which were uploaded by packages-to-gcs__at__host.sh

set -o nounset
set -o errexit

readonly SCRIPTDIR=$(dirname $0)

# Pull in global properties
source project_properties.sh

# Pull in common functions
source $SCRIPTDIR/common_utils.sh

# Remove packages from GCS
emit ""
emit "Removing packages:"
gsutil rm -R -f gs://$GCS_PACKAGE_BUCKET/$GCS_PACKAGE_DIR

emit ""
emit "Package removal complete"

