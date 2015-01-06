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

function pkgutil_get_list() {
  local pkg_dir="$1"

  find $pkg_dir -mindepth 2 -maxdepth 2 | sort
}
readonly -f pkgutil_get_list

function pkgutil_pkg_name() {
  local pkg_dir="$1"
  local pkg="$2"

  # Strip the "package" directory
  local pkg_stripped=${pkg#$pkg_dir/}

  # Get the query-tool specific directory name
  echo ${pkg_stripped%/*}
}
readonly -f pkgutil_pkg_name

function pkgutil_pkg_file() {
  local pkg_dir="$1"
  local pkg="$2"

  # Return just the filename
  echo ${pkg##*/}
}
readonly -f pkgutil_pkg_file

function pkgutil_emit_list() {
  local pkg_dir="$1"
  local pkg_list="$2"

  emit ""
  emit "Discovered packages:"
  for pkg in $pkg_list; do
    # Get the query-tool specific directory name
    local pkg_name=$(pkgutil_pkg_name $pkg_dir $pkg)

    # Get the name of the zip file
    local pkg_file=$(pkgutil_pkg_file $pkg_dir $pkg)

    emit "  $pkg_name ($pkg_file)"
  done
}
readonly -f pkgutil_emit_list

