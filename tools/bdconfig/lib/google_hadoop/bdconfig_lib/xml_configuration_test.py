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

"""Unit tests for xml_configuration.py."""

import path_initializer
path_initializer.InitializeSysPath()

import os
import textwrap

import gflags as flags
from google.apputils import basetest
import unittest

from bdconfig_lib import xml_configuration

FLAGS = flags.FLAGS


class XmlConfigurationTest(unittest.TestCase):

  def setUp(self):
    self._test_filename = os.path.join(FLAGS.test_tmpdir, 'core-site.xml')

    # Create an empty xml file for xml_helper to modify; make it lack
    # formatting such as newlines.
    file_dir = os.path.dirname(self._test_filename)
    if not os.path.exists(file_dir):
      os.makedirs(file_dir)

    self._empty_conf = unicode(
        '<?xml version="1.0" ?>'
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
        '<configuration/>')

  def testEmptyConfiguration(self):
    """Test creation of empty config"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    self.assertEqual(self._empty_conf, conf.ToXml())

  def testFromString(self):
    """Test creation of from string"""
    conf = xml_configuration.Configuration.FromString(self._empty_conf)
    self.assertEqual(self._empty_conf, conf.ToXml())

    longer_conf = unicode(
        '<?xml version="1.0" ?>'
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
        '<configuration>'
        '<property>'
        '<name>foo</name>'
        '<value>bar</value>'
        '</property>'
        '<property>'
        '<name>foo2</name>'
        '<value></value>'
        '<description>Something or another</description>'
        '</property>'
        '</configuration>')

    conf = xml_configuration.Configuration.FromString(longer_conf)
    self.assertEqual(longer_conf, conf.ToXml())

  def testUncompactedFromString(self):
    """Test creation of from string"""
    long_conf = unicode(
        '<?xml version="1.0" ?>'
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
        '<configuration>'
        '    '
        '<property>'
        '<name>foo</name>'
        '    '
        '<value>bar</value>'
        '</property>'
        '    '
        '<property>'
        '<name>   foo2</name>'
        '<value></value>'
        '<description>        Something or another</description>'
        '    '
        '</property>'
        '</configuration>')

    conf = (
        xml_configuration.Configuration.FromString(
            long_conf, compact_document=False))
    self.assertEqual(long_conf, conf.ToXml())

  def testLoadFromFile(self):
    """Basic reading from file"""
    with open(self._test_filename, 'w') as f:
      f.write(self._empty_conf)

    conf = xml_configuration.Configuration.FromFile(self._test_filename)
    self.assertEqual(self._empty_conf, conf.ToXml())

    os.remove(self._test_filename)

  def testWriteToFile(self):
    """Basic writing to file"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    conf.WriteToFile(self._test_filename)

    with open(self._test_filename, 'r') as f:
      text = f.read()

    self.assertEqual(conf.ToPrettyXml(), text)

    os.remove(self._test_filename)

  def testSetProperty(self):
    """Basic property setting"""
    conf = xml_configuration.Configuration.EmptyConfiguration()

    conf.SetProperty('key1', 'value1')
    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual(None, conf.GetPropertyDescription('key1'))

    # clobber True by default
    conf.SetProperty('key1', 'value2', description='foo')
    self.assertEqual('value2', conf.GetPropertyValue('key1'))
    self.assertEqual('foo', conf.GetPropertyDescription('key1'))

    conf.SetProperty('key1', 'value3', description='bar', clobber=False)
    self.assertEqual('value2', conf.GetPropertyValue('key1'))
    self.assertEqual('foo', conf.GetPropertyDescription('key1'))

  def testRemoveProperty(self):
    """Basic property removal"""
    conf = xml_configuration.Configuration.EmptyConfiguration()

    conf.SetProperty('key1', 'value1')
    conf.SetProperty('key2', 'value2')
    self.assertEqual(2, conf.GetNumProperties())

    conf.RemoveProperty('key1')
    self.assertEqual(None, conf.GetPropertyValue('key1'))
    self.assertEqual(1, conf.GetNumProperties())

  def testMerge(self):
    """Merge without clobber option"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    other_conf = xml_configuration.Configuration.EmptyConfiguration()

    conf.SetProperty('key1', 'value1')
    conf.SetProperty('key2', 'value2')
    other_conf.SetProperty('key2', 'foobar')
    other_conf.SetProperty('key3', 'value3')

    conf.Merge(other_conf)

    # clobber False by default
    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2', conf.GetPropertyValue('key2'))
    self.assertEqual('value3', conf.GetPropertyValue('key3'))

  def testMergeWithClobber(self):
    """Merge with clobber option"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    other_conf = xml_configuration.Configuration.EmptyConfiguration()

    conf.SetProperty('key1', 'value1')
    conf.SetProperty('key2', 'value2')
    other_conf.SetProperty('key2', 'foobar')
    other_conf.SetProperty('key3', 'value3')

    conf.Merge(other_conf, clobber=True)
    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('foobar', conf.GetPropertyValue('key2'))
    self.assertEqual('value3', conf.GetPropertyValue('key3'))

  def testNoUpdates(self):
    """Basic empty call to Update"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    properties_to_update = {}
    optional_properties_to_add = {}
    conf.Update(properties_to_update, optional_properties_to_add)
    self.assertEqual(0, conf.GetNumProperties())

  def testUpdateBasicAddAndRemove(self):
    """Basic addition and removal"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    properties_to_update = {
        'key1': 'value1',
        'key2': 'value2',
    }
    optional_properties_to_add = {}

    conf.Update(properties_to_update, optional_properties_to_add)

    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2', conf.GetPropertyValue('key2'))
    self.assertEqual(2, conf.GetNumProperties())

    # Shouldn't crash if we query for non-existent keys.
    self.assertEqual(None, conf.GetPropertyValue('key3'))

    properties_to_update = {
        'key2': 'value2_new',
        'key3': 'value3',
    }
    conf.Update(properties_to_update, optional_properties_to_add)

    # Since we didn't mention key1, it should have remained untouched.
    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2_new', conf.GetPropertyValue('key2'))
    self.assertEqual('value3', conf.GetPropertyValue('key3'))
    self.assertEqual(3, conf.GetNumProperties())

    properties_to_update = {
        'key1': None,
        'key2': 'value2_new2',
    }
    conf.Update(properties_to_update, optional_properties_to_add)

    # Explicitly setting key1 to None should delete it.
    self.assertEqual(None, conf.GetPropertyValue('key1'))
    self.assertEqual('value2_new2', conf.GetPropertyValue('key2'))
    self.assertEqual('value3', conf.GetPropertyValue('key3'))
    self.assertEqual(2, conf.GetNumProperties())

    # Now delete all properties, including some nonexistent one.
    properties_to_update = {
        'nonexistent_key': None,
        'key2': None,
        'key3': None,
    }

    conf.Update(properties_to_update, optional_properties_to_add)

    self.assertEqual(None, conf.GetPropertyValue('key1'))
    self.assertEqual(None, conf.GetPropertyValue('key2'))
    self.assertEqual(None, conf.GetPropertyValue('key3'))
    self.assertEqual(0, conf.GetNumProperties())

  def testUpdateOptionalAddAndRemove(self):
    """Properties takes precendence over pptional properties"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    properties_to_update = {}
    optional_properties_to_add = {
        'key1': 'value1',
        'key2': 'value2',
    }

    conf.Update(properties_to_update, optional_properties_to_add)

    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2', conf.GetPropertyValue('key2'))
    self.assertEqual(2, conf.GetNumProperties())

    optional_properties_to_add = {
        'key2': 'value2_new',
        'key3': 'value3',
    }

    conf.Update(properties_to_update, optional_properties_to_add)

    # value2 unchanged, value3 added.
    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2', conf.GetPropertyValue('key2'))
    self.assertEqual('value3', conf.GetPropertyValue('key3'))
    self.assertEqual(3, conf.GetNumProperties())

    # It is an error to true to 'delete' a property using the
    # optional_properties_to_add.
    optional_properties_to_add = {
        'key1': None,
    }
    self.assertRaises(
        LookupError, conf.Update,
        properties_to_update, optional_properties_to_add)

    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2', conf.GetPropertyValue('key2'))
    self.assertEqual('value3', conf.GetPropertyValue('key3'))
    self.assertEqual(3, conf.GetNumProperties())

  def testUpdateAndOptionalAdd(self):
    """Optionally update an existing property plus nonexistent one."""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    properties_to_update = {
        'key1': 'value1',
        'key2': 'value2',
    }
    optional_properties_to_add = {
        'key2': 'value2_opt',
        'key3': 'value3_opt',
    }

    conf.Update(properties_to_update, optional_properties_to_add)

    # Updates are applied before optional properties, so they win if done
    # simultaneously.
    self.assertEqual('value1', conf.GetPropertyValue('key1'))
    self.assertEqual('value2', conf.GetPropertyValue('key2'))
    self.assertEqual('value3_opt', conf.GetPropertyValue('key3'))
    self.assertEqual(3, conf.GetNumProperties())

    properties_to_update = {
        'key1': None,
        'key2': None,
        'key3': None,
    }
    optional_properties_to_add = {
        'key2': 'value2_opt2',
        'key3': 'value3_opt2',
    }

    conf.Update(properties_to_update, optional_properties_to_add)

    # Setting 'None' is updates, and then adding in optional_properties_to_add
    # is equivalent to just putting the optional adds into the update instead
    # of 'None'.
    self.assertEqual(None, conf.GetPropertyValue('key1'))
    self.assertEqual('value2_opt2', conf.GetPropertyValue('key2'))
    self.assertEqual('value3_opt2', conf.GetPropertyValue('key3'))
    self.assertEqual(2, conf.GetNumProperties())

  def testEmptyProperty(self):
    """Test empty values are distinct from None"""
    conf = xml_configuration.Configuration.EmptyConfiguration()

    # Empty string is considered a valid property value (even though the XML
    # dom would return None as the text node of the property element.
    conf.SetProperty('key', '')
    self.assertEqual('', conf.GetPropertyValue('key'))

  def testResolveEnvironmentVariables(self):
    """Fail when key is non-existent"""
    xml_string = ('<configuration><envVar name="HOSTNAME" />'
                  '<envVar name="NN" /></configuration>')
    resolved_conf = '<configuration>foo-hostbar-host</configuration>'

    env_var_store = {'HOSTNAME': 'foo-host', 'NN': 'bar-host'}

    conf = xml_configuration.Configuration.FromString(
        xml_string, env_var_store=env_var_store)
    conf.ResolveEnvironmentVariables()
    self.assertEndsWith(conf.ToXml(), resolved_conf)

    # NN is missing
    env_var_store = {'HOSTNAME': 'foo-host', 'DN': 'bar-host'}

    conf = xml_configuration.Configuration.FromString(
        xml_string, env_var_store=env_var_store)
    self.assertRaises(LookupError, conf.ResolveEnvironmentVariables)

  def testValidation(self):
    """Test assertions in constructors."""
    from_string = xml_configuration.Configuration.FromString
    # Element needs to be name configuration
    xml_string = '<not-configuration/>'
    self.assertRaises(AssertionError, from_string, xml_string)

    # Property needs non-empty name
    xml_string = '<configuration><property/></configuration>'
    self.assertRaises(AssertionError, from_string, xml_string)
    xml_string = """<configuration><property><name></name>
    </property></configuration>"""
    self.assertRaises(AssertionError, from_string, xml_string)

    # Property needs to be direct child of configuration
    xml_string = """<configuration><wrapper><property><name>foo</name>
    </property></wrapper></configuration>"""
    self.assertRaises(AssertionError, from_string, xml_string)

    # Success Case
    xml_string = """<configuration><property><name>foo</name>
    </property></configuration>"""
    conf = from_string(xml_string)
    resolved_conf = ('<configuration><property><name>foo</name><value>'
                     '</value></property></configuration>')
    self.assertEndsWith(conf.ToXml(), resolved_conf)

  def testToPrettyXml(self):
    """Make sure indenting, stripping, and textwrapping works"""
    conf = xml_configuration.Configuration.EmptyConfiguration()
    long_description = textwrap.dedent("""\
    This is a very long multi-line description. This will be split into multiple
    lines by the formatter. I think this should be enough text anyways.""")
    short_description = 'Short Description.\n'
    conf.SetProperty('foo ', '\t bar', description=long_description)
    conf.SetProperty('baz ', ' bat', description=short_description)
    pretty_xml = unicode(textwrap.dedent("""\
    <?xml version="1.0" ?>
    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
    <configuration>
      <property>
        <name>foo</name>
        <value>bar</value>
        <description>
          This is a very long multi-line description. This will be split into
          multiple lines by the formatter. I think this should be enough text
          anyways.
        </description>
      </property>
      <property>
        <name>baz</name>
        <value>bat</value>
        <description>Short Description.</description>
      </property>
    </configuration>
    """))
    self.assertEqual(pretty_xml, conf.ToPrettyXml())

if __name__ == '__main__':
  unittest.main()
