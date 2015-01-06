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

"""Main configuration command classes for bdconfig."""

import path_initializer
path_initializer.InitializeSysPath()

import logging
import os
import textwrap


from google.apputils import appcommands
import gflags as flags

from bdconfig_lib import path_validator
from bdconfig_lib import xml_configuration

FLAGS = flags.FLAGS


class XmlConfigureCommandBase(appcommands.Cmd):

  """Base class for all configuration commands."""

  def __init__(self, name, flag_values):
    """Initializes a new instance of XmlConfigureCommandBase.

    Args:
      name: The name of the command.
      flag_values: The values of command line flags to be used by the command.
    """
    super(XmlConfigureCommandBase, self).__init__(name, flag_values)

    flags.DEFINE_boolean(
        'create_if_absent',
        False,
        textwrap.dedent("""\
            Whether or not to create an empty configuration file if
            'configuration_file' is unspecified. Default: False."""),
        flag_values=flag_values)

    flags.DEFINE_string(
        'configuration_file',
        None,
        'The XML configuration to edit or query. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('configuration_file', flag_values=flag_values)

    self._flags = FLAGS

  def LoadConfiguration(self):
    if os.access(self._flags.configuration_file, os.F_OK):
      conf = xml_configuration.Configuration.FromFile(
          self._flags.configuration_file)
    elif self._flags.create_if_absent:
      conf = xml_configuration.Configuration.EmptyConfiguration()
    else:
      raise IOError('Cannot find configuration file: "{0}".'.format(
          self._flags.configuration_file))
    return conf

  def WriteConfiguration(self, conf):
    if self._flags.dry_run:
      logging.info('Dry run: Printing resulting configuration to stdout.')
      print conf.ToPrettyXml()
    else:
      conf.WriteToFile(self._flags.configuration_file)

  def SetFlagsInstance(self, flags_instance):
    """Sets the FlagValues instance to be used to retrieve command flags.

    Useful for unittesting.

    Args:
      flags_instance: Instance of FlagValues to be used.
    """
    self._flags = flags_instance


class SetProperty(XmlConfigureCommandBase):

  """Sets a single configuration value."""

  def __init__(self, name, flag_values):
    super(SetProperty, self).__init__(name, flag_values)
    flags.DEFINE_boolean(
        'clobber',
        True,
        'Whether or not to override existing properties. Default: True.',
        flag_values=flag_values)

    flags.DEFINE_string(
        'description',
        None,
        'Optional description to give to the property.',
        flag_values=flag_values)

    flags.DEFINE_string(
        'name',
        None,
        'Name of property to set. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('name', flag_values=flag_values)
    flags.RegisterValidator('name', lambda name: name, flag_values=flag_values)

    flags.DEFINE_string(
        'value',
        None,
        'Value to set property to. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('value', flag_values=flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""

    conf = self.LoadConfiguration()
    conf.SetProperty(
        self._flags.name,
        self._flags.value,
        description=self._flags.description,
        clobber=self._flags.clobber)
    self.WriteConfiguration(conf)


class RemoveProperty(XmlConfigureCommandBase):

  """Removes a single configuration property."""

  def __init__(self, name, flag_values):
    super(RemoveProperty, self).__init__(name, flag_values)

    flags.DEFINE_string(
        'name',
        None,
        'Name of property to remove. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('name', flag_values=flag_values)
    flags.RegisterValidator('name', lambda name: name, flag_values=flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""

    conf = self.LoadConfiguration()
    conf.RemoveProperty(self._flags.name)
    self.WriteConfiguration(conf)


class GetPropertyValue(XmlConfigureCommandBase):

  """Prints the value of a property"""

  def __init__(self, name, flag_values):
    super(GetPropertyValue, self).__init__(name, flag_values)

    flags.DEFINE_string(
        'name',
        None,
        'Name of property to get. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('name', flag_values=flag_values)
    flags.RegisterValidator('name', lambda name: name, flag_values=flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""

    conf = self.LoadConfiguration()
    value = conf.GetPropertyValue(self._flags.name)
    logging.info('Property value is: "{0}"'.format(value))
    print value


class MergeConfigurations(XmlConfigureCommandBase):

  """Merge properties from source_configuration_file into configuration_file."""

  def __init__(self, name, flag_values):
    super(MergeConfigurations, self).__init__(name, flag_values)
    flags.DEFINE_boolean(
        'clobber',
        False,
        'Whether or not to override existing properties. Default: False.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('clobber', flag_values=flag_values)

    flags.DEFINE_string(
        'source_configuration_file',
        None,
        'XML configuration to take properties from.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired(
        'source_configuration_file', flag_values=flag_values)
    flags.RegisterValidator(
        'source_configuration_file', lambda file: os.access(file, os.R_OK),
        flag_values=flag_values)

    flags.DEFINE_boolean(
        'resolve_environment_variables',
        False,
        textwrap.dedent("""\
            Whether or not environment variables in the configuration should
            be resolved. Default: False."""),
        flag_values=flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""

    conf = self.LoadConfiguration()
    source_conf = xml_configuration.Configuration.FromFile(
        self._flags.source_configuration_file)
    conf.Merge(source_conf, clobber=self._flags.clobber)
    if self._flags.resolve_environment_variables:
      conf.ResolveEnvironmentVariables()
    self.WriteConfiguration(conf)


class ResolveEnvironmentVariables(XmlConfigureCommandBase):

  """Replaces <envVar> tags in a template with the corresponding environment
  variables."""

  def __init__(self, name, flag_values):
    super(ResolveEnvironmentVariables, self).__init__(name, flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""

    conf = self.LoadConfiguration()
    conf.ResolveEnvironmentVariables()
    self.WriteConfiguration(conf)
