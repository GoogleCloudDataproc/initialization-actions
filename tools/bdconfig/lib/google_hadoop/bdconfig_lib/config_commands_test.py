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

"""Unit tests for config_commands.py."""

import path_initializer
path_initializer.InitializeSysPath()

import copy
import os

import gflags as flags

from google.apputils import basetest
import unittest

from bdconfig_lib import config_commands
from bdconfig_lib import xml_configuration

FLAGS = flags.FLAGS


class ConfigCommandsTestBase(unittest.TestCase):

  def setUp(self):
    # We will pretend FLAGS.test_tmpdir is the hadoop installation dir.
    self._conf_dir = os.path.join(FLAGS.test_tmpdir, 'conf')
    if not os.path.exists(self._conf_dir):
      os.makedirs(self._conf_dir)

    self._core_site_filename = os.path.join(self._conf_dir, 'core-site.xml')
    self._mapred_site_filename = os.path.join(self._conf_dir, 'mapred-site.xml')
    self._hdfs_site_filename = os.path.join(self._conf_dir, 'hdfs-site.xml')
    self._hadoop_env_filename = os.path.join(self._conf_dir, 'hadoop-env.sh')
    self._fake_jarfile = os.path.join(self._conf_dir, 'fake-jarfile.jar')

    self._CreateXmlFile(self._core_site_filename)
    self._CreateXmlFile(self._mapred_site_filename)
    self._CreateXmlFile(self._hdfs_site_filename)
    with open(self._hadoop_env_filename, 'w') as f:
      f.write(
          '#export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:' + self._fake_jarfile)
    with open(self._fake_jarfile, 'w') as f:
      f.write('Fake jarfile.')

    self._flag_values_copy = copy.deepcopy(FLAGS)

    # Derived classes define self._test_cmd for their particular appcommand.
    self._test_cmd = None

  def testValidationFailsAfterRegistrationBeforeSettingValues(self):
    """Check flag validation fails after appcommand has registered its flags."""
    if self._test_cmd:
      # Empty flags right after setup/registration should fail validator.
      with self.assertRaises(flags.IllegalFlagValue):
        self._flag_values_copy._AssertAllValidators()

  def testDryRun(self):
    """Test --dry_run prevents modifications from being committed."""
    if not self._test_cmd:
      return
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = True
    self._test_cmd.Run(None)

    # Nothing added.
    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(0, conf.GetNumProperties())
    conf = xml_configuration.Configuration.FromFile(self._mapred_site_filename)
    self.assertEqual(0, conf.GetNumProperties())
    with open(self._hadoop_env_filename, 'r') as f:
      self.assertEqual(1, len(f.readlines()))

  def tearDown(self):
    os.remove(self._core_site_filename)
    os.remove(self._mapred_site_filename)
    os.remove(self._hdfs_site_filename)
    os.remove(self._hadoop_env_filename)

  def _SetDefaultValidFlags(self):
    """Concrete classes should override to provide set of valid flags."""
    pass

  def _CreateXmlFile(self, filename):
    """Helper for creating empty XML file.

    Args:
      filename: The fully-qualified file name to create.
    """
    with open(filename, 'w') as f:
      f.write('<?xml version="1.0"?>')
      f.write('<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>')
      f.write('<configuration/>')

  def _ValidateFlagFailures(self, failure_modes):
    """Helper for iterating over a list of expected illegal flag values.

    Args:
      failure_modes: List of flag/value pairs which should cause an error.
    """
    for failure_arg in failure_modes:
      with self.assertRaises(flags.IllegalFlagValue):
        print 'Validating expected failure setting %s = %s' % (
            failure_arg[0], failure_arg[1])
        self._flag_values_copy.__setattr__(failure_arg[0], failure_arg[1])


class ConfigureHadoopTest(ConfigCommandsTestBase):

  def setUp(self):
    super(ConfigureHadoopTest, self).setUp()

    # Before instantiating the command instance, there are no required flags.
    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = config_commands.ConfigureHadoop(
        'configure_hadoop', self._flag_values_copy)

    # Point the command instance at our own flags reference for runtime values.
    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_hadoop."""
    # Validators get invoked on __setattr__ (overload for '=').
    failure_modes = [
        ('hadoop_conf_dir', '/unreadable/path'),
        ('hadoop_conf_dir', None),
        ('java_home', '/unreadable/path'),
        ('java_home', None),
        ('hadoop_tmp_dir', '/unreadable/path'),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testNormalOperation(self):
    """Test configure_hadoop values wire through to correct xml keys."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False
    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._mapred_site_filename)
    self.assertEqual(6, conf.GetNumProperties())
    self.assertEquals('foo-host:9101', conf.GetPropertyValue(
        'mapred.job.tracker'))
    self.assertEquals('foo opts', conf.GetPropertyValue(
        'mapred.child.java.opts'))
    self.assertEquals('1', conf.GetPropertyValue(
        'mapred.map.tasks'))
    self.assertEquals('2', conf.GetPropertyValue(
        'mapred.reduce.tasks'))
    self.assertEquals('3', conf.GetPropertyValue(
        'mapred.tasktracker.map.tasks.maximum'))
    self.assertEquals('4', conf.GetPropertyValue(
        'mapred.tasktracker.reduce.tasks.maximum'))

    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(1, conf.GetNumProperties())
    self.assertEquals(FLAGS.test_tmpdir, conf.GetPropertyValue(
        'hadoop.tmp.dir'))

    with open(self._hadoop_env_filename, 'r') as f:
      self.assertEqual(2, len(f.readlines()))

  def _SetDefaultValidFlags(self):
    # Good set of flags.
    self._flag_values_copy.hadoop_conf_dir = self._conf_dir
    self._flag_values_copy.java_home = FLAGS.test_tmpdir
    self._flag_values_copy.job_tracker_uri = 'foo-host:9101'
    self._flag_values_copy.java_opts = 'foo opts'
    self._flag_values_copy.default_num_maps = 1
    self._flag_values_copy.default_num_reduces = 2
    self._flag_values_copy.map_slots = 3
    self._flag_values_copy.reduce_slots = 4
    self._flag_values_copy.hadoop_tmp_dir = FLAGS.test_tmpdir

    self._flag_values_copy._AssertAllValidators()


class ConfigureGhfsTest(ConfigCommandsTestBase):

  def setUp(self):
    super(ConfigureGhfsTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = config_commands.ConfigureGhfs(
        'configure_ghfs', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_ghfs."""
    # Validators.
    failure_modes = [
        ('hadoop_conf_dir', '/unreadable/path'),
        ('hadoop_conf_dir', None),
        ('ghfs_jar_path', '/unreadable/path'),
        ('ghfs_jar_path', self._conf_dir),
        ('ghfs_jar_path', None),
        ('system_bucket', None),
        ('enable_service_account_auth', None),
        ('project_id', None),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testNormalOperation(self):
    """Test configure_ghfs values wire through to correct xml keys."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False
    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(6, conf.GetNumProperties())
    self.assertEquals('foo-bucket', conf.GetPropertyValue(
        'fs.gs.system.bucket'))
    self.assertIsNotNone(conf.GetPropertyValue(
        'fs.gs.impl'))
    self.assertEquals('true', conf.GetPropertyValue(
        'fs.gs.auth.service.account.enable'))
    self.assertEquals('google.com:myproject', conf.GetPropertyValue(
        'fs.gs.project.id'))
    self.assertEquals('1', conf.GetPropertyValue(
        'fs.gs.io.buffersize'))
    self.assertIsNotNone(conf.GetPropertyValue(
        'fs.gs.working.dir'))

    with open(self._hadoop_env_filename, 'r') as f:
      self.assertEqual(2, len(f.readlines()))

  def testReconfiguringDoesntReappendLineToEnvFile(self):
    """Calling configure_ghfs twice shouldn't re-add lines to hadoop-env.sh."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False
    self._test_cmd.Run(None)
    self._test_cmd.Run(None)

    with open(self._hadoop_env_filename, 'r') as f:
      self.assertEqual(2, len(f.readlines()))

  def testServiceAccountRequiresClientIdAndSecret(self):
    """Setting enable_service_account_auth adds extra validators."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    # If we set enable_service_account_auth to False, we now require client_id
    # and client_secret.
    self._flag_values_copy.enable_service_account_auth = False
    self.assertRaises(flags.IllegalFlagValue, self._test_cmd.Run, None)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy.__setattr__,
        'client_id', None)
    self._flag_values_copy.client_id = 'my-client-id'

    self.assertRaises(flags.IllegalFlagValue, self._test_cmd.Run, None)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy.__setattr__,
        'client_secret', None)
    self._flag_values_copy.client_secret = 'my-client-secret'

    self._test_cmd.Run(None)
    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(8, conf.GetNumProperties())
    self.assertEquals('false', conf.GetPropertyValue(
        'fs.gs.auth.service.account.enable'))
    self.assertEquals('my-client-id', conf.GetPropertyValue(
        'fs.gs.auth.client.id'))
    self.assertEquals('my-client-secret', conf.GetPropertyValue(
        'fs.gs.auth.client.secret'))

  def _SetDefaultValidFlags(self):
    self._flag_values_copy.hadoop_conf_dir = self._conf_dir
    self._flag_values_copy.ghfs_jar_path = self._fake_jarfile
    self._flag_values_copy.system_bucket = 'foo-bucket'
    self._flag_values_copy.ghfs_buffer_size = 1
    self._flag_values_copy.enable_service_account_auth = True
    self._flag_values_copy.project_id = 'google.com:myproject'

    # As long as enable_service_account_auth is True, we don't need client_id
    # and client_secret.
    self._flag_values_copy.client_id = None
    self._flag_values_copy.client_secret = None

    self._flag_values_copy._AssertAllValidators()


class ConfigureHdfsTest(ConfigCommandsTestBase):

  def setUp(self):
    super(ConfigureHdfsTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = config_commands.ConfigureHdfs(
        'configure_hdfs', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_hdfs."""
    # Validators.
    failure_modes = [
        ('hadoop_conf_dir', '/unreadable/path'),
        ('hadoop_conf_dir', None),
        ('hdfs_data_dirs', None),
        ('hdfs_data_dirs', FLAGS.test_tmpdir + ' /unreadable.path'),
        ('hdfs_name_dir', None),
        ('hdfs_name_dir', 'conf'),
        ('namenode_uri', None),
        ('namenode_uri', 'badscheme://localhost:8020/'),
        # Missing trailing slash.
        ('namenode_uri', 'hdfs://localhost:8020'),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testNormalOperation(self):
    """Test configure_hdfs values wire through to correct xml keys."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False
    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._hdfs_site_filename)
    self.assertEqual(3, conf.GetNumProperties())

    self.assertEqual(
        ','.join(self._flag_values_copy.hdfs_data_dirs),
        conf.GetPropertyValue('dfs.data.dir'))

    self.assertEqual(
        self._flag_values_copy.hdfs_name_dir,
        conf.GetPropertyValue('dfs.name.dir'))

    self.assertEqual(
        self._flag_values_copy.namenode_uri,
        conf.GetPropertyValue('dfs.namenode.rpc-address'))

  def _SetDefaultValidFlags(self):
    self._flag_values_copy.hadoop_conf_dir = self._conf_dir
    self._flag_values_copy.hdfs_data_dirs = [FLAGS.test_tmpdir, self._conf_dir]
    self._flag_values_copy.hdfs_name_dir = '/tmp'
    self._flag_values_copy.namenode_uri = 'hdfs://localhost:8020/'

    self._flag_values_copy._AssertAllValidators()


class SetDefaultFileSystemTest(ConfigCommandsTestBase):

  def setUp(self):
    super(SetDefaultFileSystemTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = config_commands.SetDefaultFileSystem(
        'set_default_fs', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for set_default_fs."""
    # Validators.
    failure_modes = [
        ('hadoop_conf_dir', '/unreadable/path'),
        ('hadoop_conf_dir', None),
        ('default_fs', None),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testNormalOperationWithSystemBucketFallback(self):
    """Test set_default_fs values wire through to correct xml keys."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.default_bucket = None
    system_bucket = 'system-bucket'
    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    conf.Update({'fs.gs.system.bucket': system_bucket}, {})
    conf.WriteToFile(self._core_site_filename)

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(2, conf.GetNumProperties())

    self.assertEqual(
        'gs://' + system_bucket + '/',
        conf.GetPropertyValue('fs.default.name'))

  def testSystemBucketFallbackFailure(self):
    """Without default_bucket, attempts and fails to fetch system_bucket."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.default_bucket = None
    # Default_bucket fails to be set using fs.gs.system.bucket without the
    # corresponding value already present in the xml file.
    self.assertRaises(KeyError, self._test_cmd.Run, None)

  def testNormalOperationWithDefaultBucket(self):
    """With default_bucket provided, no fallback, check normal wiring."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.default_bucket = 'some-other-bucket'
    system_bucket = 'system-bucket'
    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    conf.Update({'fs.gs.system.bucket': system_bucket}, {})
    conf.WriteToFile(self._core_site_filename)

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(2, conf.GetNumProperties())
    self.assertEqual(
        'gs://' + self._flag_values_copy.default_bucket + '/',
        conf.GetPropertyValue('fs.default.name'))

  def testHdfsSpecificValidators(self):
    """If default_fs is hdfs, then we add extra validators."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    # Using hdfs will invoke new validators.
    self._flag_values_copy.default_fs = 'hdfs'

    # namenode_uri fails to be set using hdfs-site.xml
    self.assertRaises(KeyError, self._test_cmd.Run, None)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy.__setattr__,
        'namenode_uri', 'badscheme://localhost:8020/')
    # Missing trailing slash.
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy.__setattr__,
        'namenode_uri', 'hdfs://localhost:8020')

  def testHdfsFallbackToNamenodeRpcAddressFailure(self):
    """With hdfs, absence of fs.default.name fetches rpc-address and fails."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.default_fs = 'hdfs'

    conf = xml_configuration.Configuration.FromFile(self._hdfs_site_filename)
    conf.Update({'dfs.namenode.rpc-address': 'badscheme://localhost:8020/'}, {})
    conf.WriteToFile(self._hdfs_site_filename)
    self.assertRaises(flags.IllegalFlagValue, self._test_cmd.Run, None)
    conf.Update({'dfs.namenode.rpc-address': 'hdfs://localhost:8020'}, {})
    conf.WriteToFile(self._hdfs_site_filename)
    self.assertRaises(flags.IllegalFlagValue, self._test_cmd.Run, None)

  def testHdfsFallbackToNamenodeRpcAddressSuccess(self):
    """With hdfs, absence of fs.default.name successfully uses rpc-address."""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.default_fs = 'hdfs'

    self._flag_values_copy.namenode_uri = None

    namenode_uri = 'hdfs://localhost:8020/'
    conf = xml_configuration.Configuration.FromFile(self._hdfs_site_filename)
    conf.Update({'dfs.namenode.rpc-address': namenode_uri}, {})
    conf.WriteToFile(self._hdfs_site_filename)

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(1, conf.GetNumProperties())

    self.assertEqual(
        namenode_uri, conf.GetPropertyValue('fs.default.name'))

    self._flag_values_copy.namenode_uri = 'hdfs://some-other-host:8020/'
    self._test_cmd.Run(None)
    conf = xml_configuration.Configuration.FromFile(self._core_site_filename)
    self.assertEqual(1, conf.GetNumProperties())
    self.assertEqual(
        self._flag_values_copy.namenode_uri,
        conf.GetPropertyValue('fs.default.name'))

  def _SetDefaultValidFlags(self):
    self._flag_values_copy.hadoop_conf_dir = self._conf_dir
    self._flag_values_copy.default_fs = 'gs'
    self._flag_values_copy.default_bucket = 'foo-bucket'
    self._flag_values_copy.namenode_uri = None

    self._flag_values_copy._AssertAllValidators()


if __name__ == '__main__':
  unittest.main()
