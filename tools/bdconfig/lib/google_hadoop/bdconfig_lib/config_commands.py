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

# Global flags which apply to all bdconfig subcommands.
flags.DEFINE_bool(
    'dry_run',
    False,
    textwrap.dedent("""\
        If true, configuration files will not be changed.
        File updates will be displayed on console.
        """))

logging.getLogger().setLevel(logging.INFO)


def AddLineToFile(file_name, line, dry_run):
  """Appends the given line to the given file if it is not already present."""
  with open(file_name, 'a+') as f:
    line = line.strip()
    if line in (f_line.strip() for f_line in f):
      logging.warn(
          'Line: "{0}" already in file "{1}"; not appending to file'.format(
              line, file_name))
    elif dry_run:
      print 'Appending line "{0}" to file "{1}"'.format(line, file_name)
    else:
      f.write(os.linesep)
      f.write(line)
      f.write(os.linesep)
      logging.info('Updated file: "{0}"'.format(file_name))


# TODO(user): Create constants for flag names
# Classes corresponding to each possible command to be run by bdconfig.
class ConfigureCommandBase(appcommands.Cmd):

  """Base class for all configuration commands."""

  def __init__(self, name, flag_values):
    """Initializes a new instance of ConfigureCommandBase.

    Args:
      name: The name of the command.
      flag_values: The values of command line flags to be used by the command.
    """
    super(ConfigureCommandBase, self).__init__(name, flag_values)

    flags.DEFINE_string(
        'hadoop_conf_dir',
        None,
        'Full path of Hadoop configuration directory. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('hadoop_conf_dir', flag_values=flag_values)
    flags.RegisterValidator(
        'hadoop_conf_dir', path_validator.AbsoluteDirectoryPath,
        flag_values=flag_values)

    self._flags = FLAGS

  def SetFlagsInstance(self, flags_instance):
    """Sets the FlagValues instance to be used to retrieve command flags.

    Useful for unittesting.

    Args:
      flags_instance: Instance of FlagValues to be used.
    """
    self._flags = flags_instance


class ConfigureHadoop(ConfigureCommandBase):

  """DEPRECATED. Use merge_configurations with the provided *-site.xml.template
  files instead.

  Sets up core Hadoop configuration independent of filesystems.

  Core configuration includes setting up URIs for the JobTracker, setting
  reasonable values/defaults for map/reduce slots, setting various paths and tmp
  directories that Hadoop will use, and providing suitable jvm options.
  """

  def __init__(self, name, flag_values):
    super(ConfigureHadoop, self).__init__(name, flag_values)
    flags.DEFINE_string(
        'java_home',
        None,
        'Full path of JRE or JDK installation directory. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('java_home', flag_values=flag_values)
    flags.RegisterValidator(
        'java_home', path_validator.AbsoluteDirectoryPath,
        flag_values=flag_values)

    flags.DEFINE_string(
        'job_tracker_uri',
        None,
        'The host and port that the MapReduce job tracker runs at.',
        flag_values=flag_values)

    flags.DEFINE_string(
        'java_opts',
        None,
        'Java opts for the task tracker child processes',
        flag_values=flag_values)

    flags.DEFINE_integer(
        'default_num_maps',
        None,
        'The default number of map tasks per job.',
        flag_values=flag_values)

    flags.DEFINE_integer(
        'default_num_reduces',
        None,
        'The default number of reduce tasks per job.',
        flag_values=flag_values)

    flags.DEFINE_integer(
        'map_slots',
        None,
        textwrap.dedent("""\
            The maximum number of map tasks that will be run simultaneously by a
            task tracker.
            """),
        flag_values=flag_values)

    flags.DEFINE_integer(
        'reduce_slots',
        None,
        textwrap.dedent("""\
            The maximum number of reduce tasks that will be run simultaneously
            by a task tracker.
            """),
        flag_values=flag_values)

    flags.DEFINE_string(
        'hadoop_tmp_dir',
        None,
        textwrap.dedent("""\
            Absolute ambi-scheme path to set for hadoop.tmp.dir; sometimes
            Hadoop uses this value as a path following the 'default' filesystem,
            and other times it is explicitly used as a local filesystem path.
            Therefore, this path must not include a scheme and must be suitable
            for all possible filesystem schemes.
            """),
        flag_values=flag_values)
    flags.RegisterValidator(
        'hadoop_tmp_dir', path_validator.AbsoluteDirectoryPath,
        flag_values=flag_values)

  def Run(self, unused_argv):
    self._UpdateCoreSiteXml()
    self._UpdateMapredSiteXml()
    self._UpdateHadoopEnv()

  def _UpdateCoreSiteXml(self):
    """Updates core-site.xml file."""
    file_name = os.path.join(self._flags.hadoop_conf_dir, 'core-site.xml')

    properties_to_update = {}
    optional_properties_to_add = {}

    if self._flags.hadoop_tmp_dir:
      properties_to_update['hadoop.tmp.dir'] = self._flags.hadoop_tmp_dir

    conf = xml_configuration.Configuration.FromFile(file_name)
    conf.Update(properties_to_update, optional_properties_to_add)
    if self._flags.dry_run:
      logging.info('Dry run: Printing resulting configuration to stdout.')
      print conf.ToPrettyXml()
    else:
      conf.WriteToFile(file_name)

  def _UpdateMapredSiteXml(self):
    """Updates mapred-site.xml file."""
    file_name = os.path.join(self._flags.hadoop_conf_dir, 'mapred-site.xml')

    properties_to_update = {}
    optional_properties_to_add = {}

    # TODO(user): Refactor this into dictionary.
    if self._flags.job_tracker_uri:
      properties_to_update['mapred.job.tracker'] = self._flags.job_tracker_uri
    if self._flags.java_opts:
      properties_to_update['mapred.child.java.opts'] = self._flags.java_opts
    if self._flags.default_num_maps is not None:
      properties_to_update['mapred.map.tasks'] = str(
          self._flags.default_num_maps)
    if self._flags.default_num_reduces is not None:
      properties_to_update['mapred.reduce.tasks'] = str(
          self._flags.default_num_reduces)
    if self._flags.map_slots is not None:
      properties_to_update['mapred.tasktracker.map.tasks.maximum'] = str(
          self._flags.map_slots)
    if self._flags.reduce_slots is not None:
      properties_to_update['mapred.tasktracker.reduce.tasks.maximum'] = str(
          self._flags.reduce_slots)

    conf = xml_configuration.Configuration.FromFile(file_name)
    conf.Update(properties_to_update, optional_properties_to_add)
    if self._flags.dry_run:
      logging.info('Dry run: Printing resulting configuration to stdout.')
      print conf.ToPrettyXml()
    else:
      conf.WriteToFile(file_name)

  def _UpdateHadoopEnv(self):
    """Updates hadoop-env.sh file by adding necessary exports."""
    path = os.path.join(self._flags.hadoop_conf_dir, 'hadoop-env.sh')

    exports = 'export JAVA_HOME="{0}"'.format(self._flags.java_home)
    AddLineToFile(path, exports, self._flags.dry_run)


class ConfigureGhfs(ConfigureCommandBase):

  """DEPRECATED. Use merge_configurations with the provided
  gcs*-site.xml.template files instead.

  Sets up all the necessary config values for GHFS to work.

  Mostly for providing the required values necessary for GHFS to run at all,
  such as project/service-account ids and credentials, but also for wiring
  common tunables, such as I/O buffer sizes.
  """

  def __init__(self, name, flag_values):
    super(ConfigureGhfs, self).__init__(name, flag_values)
    flags.DEFINE_string(
        'ghfs_jar_path',
        None,
        'Full path of gcs-connector-<version>.jar file. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('ghfs_jar_path', flag_values=flag_values)
    flags.RegisterValidator(
        'ghfs_jar_path', path_validator.AbsoluteFilePath,
        flag_values=flag_values)

    flags.DEFINE_string(
        'system_bucket',
        None,
        'Name of a GCS bucket to be used as system bucket. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('system_bucket', flag_values=flag_values)

    flags.DEFINE_integer(
        'ghfs_buffer_size',
        None,
        textwrap.dedent("""\
            Size of GHFS buffer in number of bytes. Optional.
            Ignored if not set or not positive.
            """),
        flag_values=flag_values)

    flags.DEFINE_bool(
        'enable_service_account_auth',
        True,
        'If true, enables service account authentication on GCE. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired(
        'enable_service_account_auth', flag_values=flag_values)

    flags.DEFINE_string(
        'project_id',
        None,
        'Numeric or human-readable text project ID for GCS access. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('project_id', flag_values=flag_values)

    flags.DEFINE_string(
        'client_id',
        None,
        textwrap.dedent("""\
            OAuth client ID used to access GCS.
            Required if enable_service_account_auth is false.
            """),
        flag_values=flag_values)

    flags.DEFINE_string(
        'client_secret',
        None,
        textwrap.dedent("""\
            OAuth client secret used to access GCS.
            Required if enable_service_account_auth is false.
            """),
        flag_values=flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""
    if not self._flags.enable_service_account_auth:
      flags.MarkFlagAsRequired('client_id', flag_values=self._command_flags)
      # Resetting the flag from itself triggers newly added validators.
      self._flags.client_id = self._flags.client_id

      flags.MarkFlagAsRequired('client_secret', flag_values=self._command_flags)
      # Resetting the flag from itself triggers newly added validators.
      self._flags.client_secret = self._flags.client_secret

    self._UpdateCoreSiteXml()
    self._UpdateHadoopEnv()

  def _UpdateCoreSiteXml(self):
    """Updates core-site.xml file."""
    file_name = os.path.join(self._flags.hadoop_conf_dir, 'core-site.xml')

    lflags = self._flags
    properties_to_update = {
        'fs.gs.system.bucket': lflags.system_bucket,
        'fs.gs.impl':
            'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem',
        'fs.gs.auth.service.account.enable':
            str(lflags.enable_service_account_auth).lower(),
        'fs.gs.project.id': lflags.project_id,
        'fs.gs.working.dir': '/',
    }
    if lflags.ghfs_buffer_size > 0:
      properties_to_update['fs.gs.io.buffersize'] = str(
          lflags.ghfs_buffer_size)
    if not lflags.enable_service_account_auth:
      if lflags.client_id:
        properties_to_update['fs.gs.auth.client.id'] = lflags.client_id
      if lflags.client_secret:
        properties_to_update['fs.gs.auth.client.secret'] = lflags.client_secret

    optional_properties_to_add = {}

    conf = xml_configuration.Configuration.FromFile(file_name)
    conf.Update(properties_to_update, optional_properties_to_add)
    if lflags.dry_run:
      logging.info('Dry run: Printing resulting configuration to stdout.')
      print conf.ToPrettyXml()
    else:
      conf.WriteToFile(file_name)

  def _UpdateHadoopEnv(self):
    """Updates hadoop-env.sh file by adding necessary exports."""
    path = os.path.join(self._flags.hadoop_conf_dir, 'hadoop-env.sh')

    exports = 'export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:{0}'.format(
        self._flags.ghfs_jar_path)
    AddLineToFile(path, exports, self._flags.dry_run)


class ConfigureHdfs(ConfigureCommandBase):

  """DEPRECATED. Use merge_configurations with the provided
  hdfs-site.xml.template file instead.

  Sets up all the necessary config values for HDFS to work.

  These HDFS settings are mostly specific to hdfs-site.xml, and involves
  specifying data directories and the namenode URI.
  """

  def __init__(self, name, flag_values):
    super(ConfigureHdfs, self).__init__(name, flag_values)
    flags.DEFINE_list(
        'hdfs_data_dirs',
        None,
        textwrap.dedent("""\
            Comma seperated list of full paths of datanode directories.
            Paths must not have file:// prefix. Required.,
            """),
        flag_values=flag_values)
    flags.MarkFlagAsRequired('hdfs_data_dirs', flag_values=flag_values)

    def _AbsoluteDirectoryPathListValidator(paths):
      return all(path_validator.AbsoluteDirectoryPath(path) for path in paths)
    flags.RegisterValidator(
        'hdfs_data_dirs', _AbsoluteDirectoryPathListValidator,
        flag_values=flag_values)

    flags.DEFINE_string(
        'hdfs_name_dir',
        None,
        textwrap.dedent("""\
            Path to namenode directory. Path must not have file:// prefix.
            Required.
            """),
        flag_values=flag_values)
    flags.MarkFlagAsRequired('hdfs_name_dir', flag_values=flag_values)
    flags.RegisterValidator(
        'hdfs_name_dir', path_validator.AbsolutePath,
        flag_values=flag_values)

    flags.DEFINE_string(
        'namenode_uri',
        None,
        textwrap.dedent("""\
            Fully qualified URI of HDFS namenode rpc address.
            e.g. hdfs://my-name-node:8020/. Required.
            """),
        flag_values=flag_values)
    flags.MarkFlagAsRequired('namenode_uri', flag_values=flag_values)
    flags.RegisterValidator(
        'namenode_uri', path_validator.AbsoluteHDFSUri,
        flag_values=self._command_flags)

  def Run(self, unused_argv):
    self._UpdateHdfsSiteXml()

  def _UpdateHdfsSiteXml(self):
    """Updates hdfs-site.xml file."""
    file_name = os.path.join(self._flags.hadoop_conf_dir, 'hdfs-site.xml')

    properties_to_update = {
        'dfs.data.dir': ','.join(self._flags.hdfs_data_dirs),
        'dfs.name.dir': self._flags.hdfs_name_dir,
        'dfs.namenode.rpc-address': self._flags.namenode_uri
    }
    optional_properties_to_add = {}

    conf = xml_configuration.Configuration.FromFile(file_name)
    conf.Update(properties_to_update, optional_properties_to_add)
    if self._flags.dry_run:
      logging.info('Dry run: Printing resulting configuration to stdout.')
      print conf.ToPrettyXml()
    else:
      conf.WriteToFile(file_name)


class SetDefaultFileSystem(ConfigureCommandBase):

  """DEPRECATED. Use 'set_property --configuration_file=core-site.xml
  --name=fs.default.name' instead.

  Sets the default hadoop FileSystem.

  Causes Hadoop paths which don't specify a scheme to use this default
  filesystem, e.g. /tmp -> gs://configbucket/tmp.
  """

  def __init__(self, name, flag_values):
    super(SetDefaultFileSystem, self).__init__(name, flag_values)
    flags.DEFINE_enum(
        'default_fs',
        None,
        ['gs', 'hdfs'],
        'The default filesystem to use. Required.',
        flag_values=flag_values)
    flags.MarkFlagAsRequired('default_fs', flag_values=flag_values)

    flags.DEFINE_string(
        'default_bucket',
        None,
        textwrap.dedent("""\
            Full path of a GCS bucket to use as a default for the GCS connector.
            If unspecified the value of fs.gs.system.bucket in core-site.xml
            will be used.
            """),
        flag_values=flag_values)

    flags.DEFINE_string(
        'namenode_uri',
        None,
        textwrap.dedent("""\
            Fully qualified URI of HDFS namenode. If unspecified the value of
            dfs.namenode.rpc-address in hdfs-site.xml will be used.
            """),
        flag_values=flag_values)

  def Run(self, unused_argv):
    """Main method invoked by appcommands."""
    if self._flags.default_fs == 'gs':
      self._SetGsDefault()
    elif self._flags.default_fs == 'hdfs':
      self._SetHdfsDefault()

    self._UpdateCoreSiteXml()

  def _SetGsDefault(self):
    """Helper method for dealing with gs-specific default-fs settings."""
    if not self._flags.default_bucket:
      system_bucket = self._ExtractDefaultBucketFromSystemBucket()
    else:
      system_bucket = self._flags.default_bucket

    # Setting the flag from itself triggers newly added validators.
    self._flags.default_bucket = system_bucket

  def _ExtractDefaultBucketFromSystemBucket(self):
    """Tries falling back to system_bucket due to absence of default_bucket."""
    logging.warn('--default_bucket not specified. '
                 'Checking for system bucket in core-site.xml.')
    core_site_file_name = os.path.join(
        self._flags.hadoop_conf_dir, 'core-site.xml')
    conf = xml_configuration.Configuration.FromFile(core_site_file_name)
    system_bucket = conf.GetPropertyValue('fs.gs.system.bucket')
    if not system_bucket:
      raise KeyError('fs.gs.system.bucket not found in core-site.xml')
    else:
      logging.warn('fs.gs.system.bucket found as "{0}". '
                   'Using in fs.default.name'.format(system_bucket))
      return system_bucket

  def _SetHdfsDefault(self):
    """Helper method for dealing with hdfs-specific default-fs settings."""
    flags.RegisterValidator(
        'namenode_uri', path_validator.AbsoluteHDFSUri,
        flag_values=self._command_flags)
    if not self._flags.namenode_uri:
      namenode_address = self._ExtractNamenodeUriFromRpcAddress()
    else:
      namenode_address = self._flags.namenode_uri

    # Setting the flag from itself triggers newly added validators.
    self._flags.namenode_uri = namenode_address

  def _ExtractNamenodeUriFromRpcAddress(self):
    """Tries finding namenode rpc-address as substitute for namenode uri."""
    logging.warn('--namenode_uri not specified. Checking in hdfs-site.xml')
    hdfs_site_file_name = os.path.join(
        self._flags.hadoop_conf_dir, 'hdfs-site.xml')
    conf = xml_configuration.Configuration.FromFile(hdfs_site_file_name)
    namenode_address = conf.GetPropertyValue('dfs.namenode.rpc-address')
    if not namenode_address:
      raise KeyError('dfs.namenode.rpc-address not found in hdfs-site.xml')
    else:
      logging.warn('dfs.namenode.rpc-address found as "{0}". '
                   'Using for fs.default.name'.format(namenode_address))
      return namenode_address

  def _UpdateCoreSiteXml(self):
    """Updates core-site.xml file."""
    file_name = os.path.join(self._flags.hadoop_conf_dir, 'core-site.xml')

    if self._flags.default_fs == 'gs':
      fs_name = 'gs://' + self._flags.default_bucket + '/'
    elif self._flags.default_fs == 'hdfs':
      fs_name = self._flags.namenode_uri

    properties_to_update = {
        'fs.default.name': fs_name
    }

    optional_properties_to_add = {}

    conf = xml_configuration.Configuration.FromFile(file_name)
    conf.Update(properties_to_update, optional_properties_to_add)
    if self._flags.dry_run:
      logging.info('Dry run: Printing resulting configuration to stdout.')
      print conf.ToPrettyXml()
    else:
      conf.WriteToFile(file_name)
