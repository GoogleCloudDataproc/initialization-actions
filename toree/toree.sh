#!/bin/bash

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This init script installs an Apache Toree kernel for Jupyter.
#
# This requires that the Jupyter optional component be enabled, and that
# the `toree` pip package is installed.
#
# If the `toree` pip package is not already installed, then a pinned
# version will be installed from PyPI. To control the specific version
# of toree used install it ahead of time using the `dataproc:pip.packages`
# cluster property.

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  if pip freeze | grep toree; then
    # toree is already installed
    true
  else
    # toree is not installed yet; install the latest from PyPI
    pip install --no-deps "toree==0.5.0"
  fi

  jupyter toree install --spark_home=/usr/lib/spark/
fi
