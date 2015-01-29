# Copyright 2014 Google Inc. All Rights Reserved.
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

# Installs the OpenJDK Java7 JRE using apt-get.

# Strip the debian mirrors to force only using the GCS mirrors. Not ideal for
# production usage due to stripping security.debian.org, but reduces external
# load for non-critical use cases.

if (( ${INSTALL_JDK_DEVEL} )); then
  echo 'Installing JDK with compiler and tools'
  install_application "openjdk-7-jdk" "java-1.7.0-openjdk-devel"
else
  echo 'Installing minimal JRE'
  install_application "openjdk-7-jre-headless" "java-1.7.0-openjdk"
fi
