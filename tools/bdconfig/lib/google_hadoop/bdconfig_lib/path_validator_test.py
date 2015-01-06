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

"""Unit tests for path_validator.py."""

import path_initializer
path_initializer.InitializeSysPath()

import os



import gflags as flags
import gflags_validators as flags_validators

from google.apputils import basetest
import unittest

from bdconfig_lib import path_validator

FLAGS = flags.FLAGS


class PathValidatorTest(unittest.TestCase):

  def setUp(self):
    if not os.path.exists(FLAGS.test_tmpdir):
      os.makedirs(FLAGS.test_tmpdir)

    self._test_filename = os.path.join(FLAGS.test_tmpdir, 'core-site.xml')
    with open(os.path.join(FLAGS.test_tmpdir, self._test_filename), 'w') as f:
      f.write('hello world!\n')
    self._nonexistent_dir = os.path.join(FLAGS.test_tmpdir, 'nonexistent-dir')

  def tearDown(self):
    os.remove(self._test_filename)

  def _TestAbsoluteBase(
      self, validator_func, succeed_on_nonexistent_dir,
      succeed_on_file, succeed_on_dir):
    """Helper for failure cases of absolute path validators.

    Args:
      validator_func: The function to test for basic failure modes.
      succeed_on_nonexistent_dir: Whether validator_func accepts an
          absolute non-existent directory.
      succeed_on_file: Whether validator_func accepts an
          absolute existent file.
      succeed_on_dir: Whether validator_func accepts an
          absolute existent directory.
    """
    outcomes = {
        'file:///tmp': False,  # Must not be a URL
        '../': False,  # Must be absolute
        '': False,  # Cannot be empty
        self._nonexistent_dir: succeed_on_nonexistent_dir,
        self._test_filename: succeed_on_file,
        FLAGS.test_tmpdir: succeed_on_dir,
    }

    # Allowed to not be set
    self.assertTrue(validator_func(None))

    # Test all cases
    for path in outcomes:
      expect_success = outcomes[path]
      if expect_success:
        self.assertTrue(validator_func(path))
      else:
        self.assertRaises(flags_validators.Error, validator_func, path)

  def testAbsolutePath(self):
    succeed_on_nonexistent_dir = True
    succeed_on_file = True
    succeed_on_dir = True
    self._TestAbsoluteBase(
        path_validator.AbsolutePath, succeed_on_nonexistent_dir,
        succeed_on_file, succeed_on_dir)

  def testAbsoluteReadablePath(self):
    succeed_on_nonexistent_dir = False
    succeed_on_file = True
    succeed_on_dir = True
    self._TestAbsoluteBase(
        path_validator.AbsoluteReadablePath, succeed_on_nonexistent_dir,
        succeed_on_file, succeed_on_dir)

  def testAbsoluteDirectoryPath(self):
    succeed_on_nonexistent_dir = False
    succeed_on_file = False
    succeed_on_dir = True
    self._TestAbsoluteBase(
        path_validator.AbsoluteDirectoryPath, succeed_on_nonexistent_dir,
        succeed_on_file, succeed_on_dir)

  def testAbsoluteFilePath(self):
    succeed_on_nonexistent_dir = False
    succeed_on_file = True
    succeed_on_dir = False
    self._TestAbsoluteBase(
        path_validator.AbsoluteFilePath, succeed_on_nonexistent_dir,
        succeed_on_file, succeed_on_dir)

  def testAbsoluteHDFSUri(self):
    failure_modes = [
        'file:///tmp/',  # Incorrect scheme
        'hdfs://localhost:8080',  # The path component must begin with /
        'hdfs://:8080/',  # Must have a hostname
        'hdfs://localhost/',  # Must have a port
    ]

    for failure_arg in failure_modes:
      with self.assertRaises(flags_validators.Error):
        print 'Validating expected failure: %s' % failure_arg
        path_validator.AbsoluteHDFSUri(failure_arg)

    # Basic well-formed URI.
    self.assertTrue(path_validator.AbsoluteHDFSUri('hdfs://localhost:8080/'))

    # Okay to have stuff after the path.
    self.assertTrue(
        path_validator.AbsoluteHDFSUri('hdfs://localhost:8080/foo/bar'))


if __name__ == '__main__':
  unittest.main()
