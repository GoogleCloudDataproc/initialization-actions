#!/usr/bin/env python
#
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

"""Helpers for validating Paths and URIs in flags."""

import os
import urlparse

import gflags_validators as flags_validators


def AbsolutePath(path):
  """Validates that the given path is absolute."""
  if path is None:
    # Value is allowed to not be set
    return True
  components = urlparse.urlparse(path)
  if components.scheme:
    raise flags_validators.Error(
        'The given path must not be a url: ' + path)
  elif not os.path.isabs(path):
    raise flags_validators.Error('Path must be absolute: ' + path)
  else:
    return True


def AbsoluteReadablePath(path):
  """Validates that the given path is absolute and is readable."""
  if path is None:
    # Value is allowed to not be set
    return True
  AbsolutePath(path)
  if not os.access(path, os.R_OK):
    raise flags_validators.Error('Cannot read path: ' + path)
  else:
    return True


def AbsoluteDirectoryPath(path):
  """Validates that the given path is absolute and points to a directory."""
  if path is None:
    # Value is allowed to not be set
    return True
  AbsoluteReadablePath(path)
  if not os.path.isdir(path):
    raise flags_validators.Error(
        'Path must point to an existing directory: ' + path)
  else:
    return True


def AbsoluteFilePath(path):
  """Validates that the given path is absolute and points to a file."""
  if path is None:
    # Value is allowed to not be set
    return True
  AbsoluteReadablePath(path)
  if os.path.isdir(path):
    raise flags_validators.Error(
        'Path must point to an existing file: ' + path)
  else:
    return True


def AbsoluteHDFSUri(uri):
  """Validates that the given path is absolute and looks like an HDFS path."""
  if uri is None:
    # Value is allowed to not be set
    return True
  components = urlparse.urlparse(uri)
  if components.scheme != 'hdfs':
    raise flags_validators.Error(
        'The given path must use hdfs scheme: ' + uri)
  elif not components.path.startswith('/'):
    raise flags_validators.Error(
        'The given HDFS path must be absolute: ' + uri)
  elif not components.hostname:
    raise flags_validators.Error(
        'The given HDFS path contain host name: ' + uri)
  elif not components.port:
    raise flags_validators.Error(
        'The given HDFS path contain port number: ' + uri)
  else:
    return True
