#!/bin/bash
#    Copyright 2019 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
# This script installs Apache Solr (http://lucene.apache.org/solr/) on a Google Cloud
# Dataproc cluster.

set -euxo pipefail

readonly SOLR_VERSION='7.6.0'
readonly SOLR_DOWNLOAD_LINK="https://www-us.apache.org/dist/lucene/solr/${SOLR_VERSION}/solr-${SOLR_VERSION}.tgz"

function err() {
  echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
  return 1
}

cd tmp && wget -q "${SOLR_DOWNLOAD_LINK}" && wget -q "${SOLR_DOWNLOAD_LINK}.sha512"
diff <(sha512sum solr-${SOLR_VERSION}.tgz | awk {'print $1'}) \
 <(cat solr-${SOLR_VERSION}.tgz.sha512 | awk {'print $1'}) \
 || err 'Verification of downloaded solr archive failed.'

tar -xf "solr-${SOLR_VERSION}.tgz" && pushd "solr-${SOLR_VERSION}/bin" \
  && ./install_solr_service.sh "/tmp/solr-${SOLR_VERSION}.tgz" -n && popd

rm -rf "/tmp/solr-${SOLR_VERSION}.tgz" "/tmp/solr-${SOLR_VERSION}"

systemctl start solr || err 'Unable to start solr service.'
