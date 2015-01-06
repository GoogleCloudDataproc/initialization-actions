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

"""Unit tests for xml_config_commands.py."""

import path_initializer
path_initializer.InitializeSysPath()

import copy
import os

import gflags as flags

from google.apputils import basetest
import unittest

from bdconfig_lib import config_commands
from bdconfig_lib import xml_config_commands
from bdconfig_lib import xml_configuration

FLAGS = flags.FLAGS


class XmlConfigCommandsTestBase(unittest.TestCase):

  def setUp(self):
    # We will pretend FLAGS.test_tmpdir is the hadoop installation dir.
    self._conf_dir = os.path.join(FLAGS.test_tmpdir, 'conf')
    if not os.path.exists(self._conf_dir):
      os.makedirs(self._conf_dir)

    self._config_filename = os.path.join(self._conf_dir, 'core-site.xml')
    self._template_filename = os.path.join(
        self._conf_dir, 'core-site.xml.template')

    self._template_string = unicode(
        '<?xml version="1.0" ?>'
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
        '<configuration>'
        '<property>'
        '<name>key1</name>'
        '<value><envVar name="ENV_VAR"/></value>'
        '</property>'
        '<property>'
        '<name>key2</name>'
        '<value>value2</value>'
        '</property>'
        '<property>'
        '<name>foo</name>'
        '<value>value3</value>'
        '<description>Something or another</description>'
        '</property>'
        '</configuration>')

    conf = xml_configuration.Configuration.EmptyConfiguration()
    conf.WriteToFile(self._config_filename)

    with open(self._template_filename, 'w') as f:
      f.write(self._template_string)

    self._flag_values_copy = copy.deepcopy(FLAGS)

    # Derived classes define self._test_cmd for their particular appcommand.
    self._test_cmd = None

  def testValidationFailsAfterRegistrationBeforeSettingValues(self):
    """Check flag validation fails after appcommand has registered its flags."""
    if self._test_cmd:
      # Empty flags right after setup/registration should fail validator.
      with self.assertRaises(flags.IllegalFlagValue):
        self._flag_values_copy._AssertAllValidators()

  def testNormalOperation(self):
    """Test command with default valid flags"""
    if not self._test_cmd:
      return
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False
    self._test_cmd.Run(None)

    self._ValidateNormalOperation()

  def testDryRun(self):
    """Test --dry_run prevents modifications from being committed."""
    if not self._test_cmd:
      return
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = True
    self._test_cmd.Run(None)

    # Nothing added.
    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(0, conf.GetNumProperties())
    # Nothing changed
    conf = xml_configuration.Configuration.FromFile(self._template_filename)
    self.assertEqual(self._template_string, conf.ToXml())

  def testCreateIfAbsent(self):
    """Test --create_if_absent_creates an empty config if one is not given."""
    if not self._test_cmd:
      return

    self._SetDefaultValidFlags()
    os.remove(self._flag_values_copy.configuration_file)

    self._flag_values_copy.create_if_absent = True
    self._flag_values_copy._AssertAllValidators()
    self._test_cmd.Run(None)

    self._ValidateNormalOperation()

  def tearDown(self):
    os.remove(self._config_filename)
    os.remove(self._template_filename)

  def _SetDefaultValidFlags(self):
    """Concrete classes should extend to provide set of valid flags."""
    pass

  def _ValidateNormalOperations(self):
    """Concrete classes should override to test set of valid flags."""
    pass

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


class GetPropertyValue(XmlConfigCommandsTestBase):
  # TODO(user): Test GetPropertyValue
  pass


class SetPropertyTest(XmlConfigCommandsTestBase):

  def setUp(self):
    super(SetPropertyTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = xml_config_commands.SetProperty(
        'set_property', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    # Point the command instance at our own flags reference for runtime values.
    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_hadoop."""
    # Validators get invoked on __setattr__ (overload for '=').
    failure_modes = [
        ('configuration_file', None),
        ('name', None),
        ('name', ''),
        ('value', None),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testOverWriting(self):
    """Test overwriting a property"""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.configuration_file = self._template_filename
    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._template_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('bar', conf.GetPropertyValue('foo'))

  def testNotClobbering(self):
    """Test overwriting a property"""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.configuration_file = self._template_filename
    self._flag_values_copy.clobber = False

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._template_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('value3', conf.GetPropertyValue('foo'))

  def testAppendNotClobbering(self):
    """Test overwriting a property"""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.clobber = False

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(1, conf.GetNumProperties())
    self.assertEquals('bar', conf.GetPropertyValue('foo'))

  def _SetDefaultValidFlags(self):
    # Good set of flags.
    self._flag_values_copy.configuration_file = self._config_filename
    self._flag_values_copy.name = 'foo'
    self._flag_values_copy.value = 'bar'

    self._flag_values_copy._AssertAllValidators()

  def _ValidateNormalOperation(self):
    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(1, conf.GetNumProperties())
    self.assertEquals('bar', conf.GetPropertyValue('foo'))


class RemovePropertyTest(XmlConfigCommandsTestBase):

  def setUp(self):
    super(RemovePropertyTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = xml_config_commands.RemoveProperty(
        'remove_property', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    # Point the command instance at our own flags reference for runtime values.
    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_hadoop."""
    # Validators get invoked on __setattr__ (overload for '=').
    failure_modes = [
        ('configuration_file', None),
        ('name', None),
        ('name', ''),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testBasicRemoval(self):
    """Test removing a property"""
    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.configuration_file = self._template_filename

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._template_filename)
    self.assertEqual(2, conf.GetNumProperties())
    self.assertEquals(None, conf.GetPropertyValue('foo'))

  def _SetDefaultValidFlags(self):
    # Good set of flags.
    self._flag_values_copy.configuration_file = self._config_filename
    self._flag_values_copy.name = 'foo'

    self._flag_values_copy._AssertAllValidators()

  def _ValidateNormalOperation(self):
    # Nothing to remove
    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(0, conf.GetNumProperties())


class MergeConfigurationsTest(XmlConfigCommandsTestBase):

  def setUp(self):
    super(MergeConfigurationsTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = xml_config_commands.MergeConfigurations(
        'merge_configurations', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    # Point the command instance at our own flags reference for runtime values.
    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_hadoop."""
    # Validators get invoked on __setattr__ (overload for '=').
    failure_modes = [
        ('configuration_file', None),
        ('source_configuration_file', None),
        ('source_configuration_file', 'unreadable_file'),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testBasicMerge(self):
    """Test foo is not clobbered"""
    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    conf.SetProperty('foo', 'bar')
    conf.WriteToFile(self._config_filename)

    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False
    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('bar', conf.GetPropertyValue('foo'))
    self.assertEquals('value2', conf.GetPropertyValue('key2'))

  def testClobber(self):
    """Test foo is clobbered"""
    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    conf.SetProperty('foo', 'bar')
    conf.WriteToFile(self._config_filename)

    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.clobber = True

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('value3', conf.GetPropertyValue('foo'))
    self.assertEquals('value2', conf.GetPropertyValue('key2'))

  def testEvaluateEnvironmentVariables(self):
    """Test <envVar> is evaluated"""
    os.environ['ENV_VAR'] = 'bar'

    self._SetDefaultValidFlags()
    self._flag_values_copy.dry_run = False

    self._flag_values_copy.resolve_environment_variables = True

    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('value3', conf.GetPropertyValue('foo'))
    self.assertEquals('value2', conf.GetPropertyValue('key2'))
    self.assertEquals('bar', conf.GetPropertyValue('key1'))

  def _SetDefaultValidFlags(self):
    # Good set of flags.
    self._flag_values_copy.configuration_file = self._config_filename
    self._flag_values_copy.source_configuration_file = self._template_filename

    self._flag_values_copy._AssertAllValidators()

  def _ValidateNormalOperation(self):
    conf = xml_configuration.Configuration.FromFile(self._config_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('value3', conf.GetPropertyValue('foo'))
    self.assertEquals('value2', conf.GetPropertyValue('key2'))


class ResolveEnvironmentVariablesTest(XmlConfigCommandsTestBase):

  def setUp(self):
    super(ResolveEnvironmentVariablesTest, self).setUp()

    self._flag_values_copy._AssertAllValidators()
    self._test_cmd = xml_config_commands.ResolveEnvironmentVariables(
        'resolve_environment_variables', self._flag_values_copy)
    self.assertRaises(
        flags.IllegalFlagValue, self._flag_values_copy._AssertAllValidators)

    # Point the command instance at our own flags reference for runtime values.
    self._test_cmd.SetFlagsInstance(self._flag_values_copy)

  def testFlagValidation(self):
    """Test basic flag failures for configure_hadoop."""
    # Validators get invoked on __setattr__ (overload for '=').
    failure_modes = [
        ('configuration_file', None),
    ]
    self._ValidateFlagFailures(failure_modes)

  def testCreateIfAbsent(self):
    """Test --create_if_absent_creates an empty config if one is not given."""
    if not self._test_cmd:
      return

    self._SetDefaultValidFlags()
    os.remove(self._flag_values_copy.configuration_file)

    self._flag_values_copy.create_if_absent = True
    self._flag_values_copy._AssertAllValidators()
    self._test_cmd.Run(None)

    conf = xml_configuration.Configuration.FromFile(self._template_filename)
    self.assertEqual(0, conf.GetNumProperties())

  def _SetDefaultValidFlags(self):
    # Good set of flags.
    os.environ['ENV_VAR'] = 'bar'
    self._flag_values_copy.configuration_file = self._template_filename

    self._flag_values_copy._AssertAllValidators()

  def _ValidateNormalOperation(self):
    conf = xml_configuration.Configuration.FromFile(self._template_filename)
    self.assertEqual(3, conf.GetNumProperties())
    self.assertEquals('value3', conf.GetPropertyValue('foo'))
    self.assertEquals('value2', conf.GetPropertyValue('key2'))
    self.assertEquals('bar', conf.GetPropertyValue('key1'))

if __name__ == '__main__':
  unittest.main()
