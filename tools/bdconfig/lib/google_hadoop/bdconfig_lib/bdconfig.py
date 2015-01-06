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

"""Command-line tool for configuring a Hadoop cluster on Google Cloud Platform.

Provides configuration commands for enabling the GoogleHadoopFileSystem (GHFS).
GHFS can be used as a file system that co-exists with HDFS or it can be
configured to replace HDFS. This script supports both modes of operations.
The script should be run on each node of a Hadoop cluster.
"""

import path_initializer
path_initializer.InitializeSysPath()


from google.apputils import appcommands
import gflags as flags

from bdconfig_lib import config_commands
from bdconfig_lib import xml_config_commands

# flags.ADOPT_module_key_flags(config_commands)
flags.ADOPT_module_key_flags(xml_config_commands)


def main(_):
  # The real work is performed by the appcommands.Run() method, which
  # first invokes this method, and then runs the specified command.

  # Register all the commands.
  appcommands.AddCmd('get_property_value', xml_config_commands.GetPropertyValue)
  appcommands.AddCmd('merge_configurations',
                     xml_config_commands.MergeConfigurations)
  appcommands.AddCmd('remove_property', xml_config_commands.RemoveProperty)
  appcommands.AddCmd('resolve_environment_variables',
                     xml_config_commands.ResolveEnvironmentVariables)
  appcommands.AddCmd('set_property', xml_config_commands.SetProperty)
  appcommands.AddCmd('configure_hadoop', config_commands.ConfigureHadoop)
  appcommands.AddCmd('configure_ghfs', config_commands.ConfigureGhfs)
  appcommands.AddCmd('configure_hdfs', config_commands.ConfigureHdfs)
  appcommands.AddCmd('set_default_fs', config_commands.SetDefaultFileSystem)


# Detect if we are being run directly, and if so call appcommands.Run()
if __name__ == '__main__':
  appcommands.Run()
