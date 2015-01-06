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

import logging
import os
import sys
import textwrap

"""Helpers for editing *-site.xml files associated with a Hadoop install."""

from xml import dom
from xml.dom import minidom

# The indent of the xml document
_INDENT = 2 * ' '


class Configuration(object):

  """Wrapper for a dom Document containing a Hadoop configuration."""

  class _Property(object):

    """Wrapper for a dom Element of a Hadoop configuration property.

    Attributes:
      name: The name of the property.
      value: The value of the property.
      description: The optional description of the property.
    """

    class _TextElement(object):

      """Wrapper for a dom Element containing one or more text nodes.

      Attributes:
        text: The contents of the element.
      """

      _MAX_WIDTH = 80

      _TEXT_WRAPPER = textwrap.TextWrapper(
          width=_MAX_WIDTH,
          initial_indent=3 * _INDENT,
          subsequent_indent=3 * _INDENT)

      def __init__(self, element):
        self._element = element
        if self.text is None:
          self.text = ''

      @property
      def text(self):
        # Could incude envVars
        texts = [child.toxml() for child in self._element.childNodes]
        if texts:
          return ''.join(texts)
        return None

      @text.setter
      def text(self, value):
        # Clear children
        while self._element.hasChildNodes():
          self._element.removeChild(self._element.firstChild)
        if value is not None:
          text_node = self._element.ownerDocument.createTextNode(value)
          self._element.appendChild(text_node)

      def Format(self):
        """Formats text. Only wraps descriptions."""
        if self._element.getElementsByTagName('*'):
          # Do not Destroy <envVars>
          return
        text = self.text.strip()
        if (len(text) > self._MAX_WIDTH - 27 - 2 * len(_INDENT) and
            self._element.nodeName == 'description'):
          text = '\n{0}\n{1}'.format(
              self._TEXT_WRAPPER.fill(text), 2 * _INDENT)
        self.text = text

    def __init__(self, element, name=None):
      assert(element.nodeName == 'property')
      self._element = element
      self._name = self._LoadTextElement('name', create_if_absent=True)
      self._value = self._LoadTextElement('value', create_if_absent=True)
      # Description is optional
      self._description = self._LoadTextElement('description')
      if name:
        self.name = name
      assert(self.name)

    @property
    def name(self):
      """Get the name."""
      return self._name.text

    @name.setter
    def name(self, value):
      """Sets the name."""
      self._name.text = value

    @property
    def value(self):
      """Get the value."""
      return self._value.text

    @value.setter
    def value(self, value):
      """Sets the value."""
      self._value.text = value

    @property
    def description(self):
      """Get the description."""
      if self._description:
        return self._description.text
      return None

    @description.setter
    def description(self, value):
      """Sets the description."""
      if not self._description:
        self._description = self._LoadTextElement(
            'description', create_if_absent=True)
      self._description.text = value

    def _LoadTextElement(self, element_name, create_if_absent=False):
      """Sets the description."""
      children = self._element.getElementsByTagName(element_name)
      if not children:
        if not create_if_absent:
          return None
        child = self._element.ownerDocument.createElement(element_name)
        self._element.appendChild(child)
      elif len(children) == 1:
        child = children[0]
      else:
        raise ValueError('XML element has multiple child elements named: '
                         '"{0}"'.format(element_name))
      return self._TextElement(child)

  def __init__(self, document, compact_document=True, env_var_store=None):
    """Constructor:

    Removes whitespace nodes.

    Args:
      document: The configuration document being wrapped.
      compact_document: Whether or not to clean white_space of the document.
      env_var_store: (Optional) A dictionary holding environment variable,
        key pairs.
    """
    self._document = document
    if compact_document:
      self._Compact(self._document)
      self._document.normalize()
    if env_var_store:
      self._env_var_store = env_var_store
    else:
      self._env_var_store = os.environ
    self._configuration = document.documentElement
    self._properties = dict()
    assert(self._configuration.nodeName == 'configuration')
    for element in self._configuration.getElementsByTagName('property'):
      assert(element.parentNode == self._configuration)
      property = self._Property(element)
      self._properties[property.name] = property

  @classmethod
  def FromFile(cls, file):
    """Loads the specified file as a Configuration.

    Args:
      file: File name or file object of a config XML file.

    Returns:
      The configuration
    """
    document = minidom.parse(file)
    return cls(document)

  @classmethod
  def FromString(cls, xml_string, compact_document=True, env_var_store=None):
    """Loads the specified string as a Configuration.

    Args:
      xml_string of a config XML file.
      compact_document: Whether or not to clean white_space of the document.
      env_var_store: (Optional) A dictionary holding environment variable,
        key pairs.

    Returns:
      The configuration
    """
    document = minidom.parseString(xml_string)
    return cls(document, compact_document, env_var_store)

  @classmethod
  def EmptyConfiguration(cls, env_var_store=None):
    """Loads an empty Configuration.

    Args:
      env_var_store: (Optional) A dictionary holding environment variable,
        key pairs.

    Returns:
      The configuration
    """
    document = minidom.getDOMImplementation().createDocument(
        None, 'configuration', None)
    style_sheet = document.createProcessingInstruction(
        'xml-stylesheet type="text/xsl"', 'href="configuration.xsl"')
    document.insertBefore(style_sheet, document.documentElement)
    return cls(document, env_var_store)

  def ToXml(self):
    """Returns a compact string of the underlying document.

    Returns:
      The raw text string of the value of the property requested.
    """
    return self._document.toxml()

  def ToPrettyXml(self):
    """Returns a well formatted string of the underlying document

    Returns:
      The raw text string of the value of the property requested.
    """
    self._FormatTextElements()
    pretty_xml = self._document.toprettyxml(indent=_INDENT)

    # Python 2.6 pretty_prints text nodes terribly.
    if tuple(sys.version_info[:2]) < (2, 7):
      mangled_document = self.FromString(pretty_xml, compact_document=False)
      mangled_document._FormatTextElements()
      pretty_xml = mangled_document.ToXml()
    return pretty_xml

  def WriteToFile(self, file_name):
    """Wrties a well formatted string of the underlying document to a file

    Args:
      The name of the file
    """
    with open(file_name, 'w') as f:
      f.write(self.ToPrettyXml())

  def GetNumProperties(self):
    """Returns the total number of <property> elements in the configuration.

    Returns:
      The total number of <property> elements in the configuration.
    """
    return len(self._properties)

  def GetPropertyValue(self, name):
    """Retrieves the text value of a property by name.

    Returns the raw text of the <value> node residing under a <property> element
    whose <name> contains text matching name.

    Args:
      name: The name of the property to retrieve.

    Returns:
      The raw text string of the value of the property requested.
    """
    property = self._properties.get(name)
    if property:
      return property.value
    return None

  def GetPropertyDescription(self, name):
    """Retrieves the text value of a property by name.

    Returns the raw text of the <description> node residing under a <property>
    element whose <name> contains text matching name.

    Args:
      name: The name of the property to retrieve.

    Returns:
      The raw text string of the description of the property requested.
    """
    property = self._properties.get(name)
    if property:
      return property.description
    return None

  def SetProperty(self, name, value, description=None, clobber=True):
    """Sets value and optionally description of property with given name.

    Args:
      name: The name of the property to set.
      value: The value to set the property to.
      description: (Optional) The description to give the property.
      clobber: Whether or not to overwrite existing properties. True by default.

    Returns:
      Whether or not the value was set.
    """
    if name in self._properties:
      if not clobber:
        logging.warn(
            'Property "{0}" is already set. Not clobbering.'.format(name))
        return False
      property = self._properties[name]
    else:
      property_element = self._document.createElement('property')
      property = self._Property(property_element, name=name)
      self._PutProperty(property)
    property.value = value
    if description:
      property.description = description
    return True

  def RemoveProperty(self, name):
    """Removes property with given name.

    Args:
      name: The name of the property to remove.

    Returns:
      Whether or not the value was removed.
    """
    property = self._properties.get(name)
    if property:
      self._configuration.removeChild(property._element)
      del self._properties[name]
      return True
    logging.info('Property "{0}" is not present. Not removing'.format(name))
    return False

  def Merge(self, other_conf, clobber=False):
    """Merge the properties of another configuration into this one.

    WARNING: This will remove properties from the other_conf.

    Args:
      other_conf: The other configuration.
      clobber: Whether or not to overwrite existing properties.
        False by default.
    """
    for name in other_conf._properties:
      if name in self._properties and not clobber:
        logging.info(
            'Property "{0}" is already set. Not clobbering'.format(name))
      else:
        self._PutProperty(other_conf._properties[name])

  # TODO(user): Split this into two methods or just deprecate it
  def Update(self, properties_to_update, optional_properties_to_add):
    """Updates properties in the configuration.

    Args:
      properties_to_update: Dict of properties to update.
        where,
        key = name of a property,
        value = property is set to this value.
      optional_properties_to_add: Map of properties to add
        only if it doesn't already exist in the file.

    Raises:
      LookupError: if a property lookup in optional_properties_to_add is None.
    """

    # Iterate through the properties and update their values.
    # If value == None, we remove that property from the file.
    for name, value in sorted(properties_to_update.iteritems()):
      if value is None:
        self.RemoveProperty(name)
      else:
        self.SetProperty(name, value)

    # For optional properties, we only insert values if there is no existing
    # value already; as such there is no such thing as optional removal
    # of an existing property.
    for name, value in sorted(optional_properties_to_add.iteritems()):
      if value is None:
        raise LookupError('Unexpected None for name: ' + name)
      else:
        self.SetProperty(name, value, clobber=False)

  def ResolveEnvironmentVariables(self):
    """Replace <envVar/> elements with the corresponding environment variables.

    Raises:
      KeyError: If a variable is not set or empty.
      ValueError: If an <envVar/> element is malformed.
    """
    for node in self._document.getElementsByTagName('envVar'):
      var_name = node.getAttribute('name')
      if var_name:
        value = self._env_var_store.get(var_name)
        if not value:
          raise LookupError(
              'No environment variable named "{0}" found.'.format(var_name))
        text_node = self._document.createTextNode(value)
        node.parentNode.replaceChild(text_node, node)
      else:
        raise ValueError('envVar tag has no attribute name.')
    self._document.normalize()

  @staticmethod
  def _Compact(node):
    """Compacts all whitespace that exists purely for indentation."""
    for child in node.childNodes:
      if (child.nodeType == dom.Node.TEXT_NODE and
          (node.nodeName == 'configuration' or
           node.nodeName == 'property' or
           node.nodeName == '#document')):
        child.nodeValue = ''
      Configuration._Compact(child)

  def _FormatTextElements(self):
    """Strips and text wraps text elements."""
    elements = (
        self._document.getElementsByTagName('name') +
        self._document.getElementsByTagName('value') +
        self._document.getElementsByTagName('description'))
    for element in elements:
      text_element = self._Property._TextElement(element)
      text_element.Format()

  def _PutProperty(self, property):
    """Puts a <property> element in the configuration."""
    old_property = self._properties.get(property.name)
    if old_property:
      self._configuration.replaceChild(property._element, old_property._element)
    else:
      self._configuration.appendChild(property._element)
    self._properties[property.name] = property
