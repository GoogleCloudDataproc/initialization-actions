#!/bin/bash

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
#
# This script installs ipex acceleration drivers and does not collects GPU utilization metrics at this time

set -euxo pipefail

mkdir -p /home/.cache
chmod -R 777 /home/.cache

pip install tiktoken
pip install tldextract
pip install zstandard
pip install transformers
pip install -U optimum[neural-compressor] intel-extension-for-transformers
pip install intel_extension_for_pytorch
pip install fasttext
export CUDA_VISIBLE_DEVICES=""

DEFAULT_SCALA_VER=2.12
DEFAULT_TFRECORD_VER=0.4.0
SCALA_VER=$(get_metadata_attribute 'scala-version' "${DEFAULT_SCALA_VER}")
TFRECORD_VER=$(get_metadata_attribute 'spark-tfrecord-version' "${DEFAULT_TFRECORD_VER}")

JAR_FILENAME="spark-tfrecord_${SCALA_VER}-${TFRECORD_VER}.jar"
wget "https://repo1.maven.org/maven2/com/linkedin/sparktfrecord/spark-tfrecord_${SCALA_VER}/${TFRECORD_VER}/${JAR_FILENAME}"
mv "${JAR_FILENAME}" /usr/lib/spark/jars/
