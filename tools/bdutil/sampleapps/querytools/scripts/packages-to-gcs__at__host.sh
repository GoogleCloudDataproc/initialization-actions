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

# packages-to-gcs
#   This script examines the Hadoop tools packages directory for a list
#   of packages to push to Google Cloud Storage.
#
#   All packages should be found in the "packages" subdirectory.
#   The required format is for the package name to be a subdirectory
#   and the associated TAR.GZ file to be inside the package subdirectory:
#     packages/
#          hive/
#            hive-0.10.0.tar.gz
#          pig/
#            pig-0.11.1.tar.gz

set -o nounset
set -o errexit

readonly SCRIPTDIR=$(dirname $0)

# Pull in global properties
source project_properties.sh

# Pull in common functions
source $SCRIPTDIR/common_utils.sh
source $SCRIPTDIR/package_utils.sh

# The resulting PACKAGE_LIST will contain one entry per package where the
# the entry is of the form "package_dir/package/gzip"
#    (for example packages/hive/hive-0.10.0.tar.gz)
PACKAGE_LIST=$(pkgutil_get_list $PACKAGES_DIR)
if [[ -z $PACKAGE_LIST ]]; then
  die "No package found in $PACKAGES_DIR subdirectory"
fi

# Emit package list
pkgutil_emit_list "$PACKAGES_DIR" "$PACKAGE_LIST"

# Push packages to GCS
emit ""
emit "Pushing packages to gs://$GCS_PACKAGE_BUCKET/$GCS_PACKAGE_DIR/:"
gsutil -m cp -R $PACKAGES_DIR gs://$GCS_PACKAGE_BUCKET/$GCS_PACKAGE_DIR/

emit ""
emit "Package upload complete"

