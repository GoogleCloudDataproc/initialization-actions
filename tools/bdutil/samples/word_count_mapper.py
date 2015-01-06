# Copyright 2014 Google Inc. All Rights Reserved.
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
"""Mapper for use with hadoop-streaming bigquery word-count example.

Reads each line of input and writes out lines each containing
a single word and the number 1.
The input lines consist of two tab-separated fields:
  1. the record number
  2. JSON data
We pick one field of the JSON and use its value as the word to output.
"""

import re
import sys


def main(args):
  # Set up the pattern that we use to extract our field
  field_name = args[1]
  field_pattern = '\\{.*"(' + field_name + ')":"([^"]*)".*\\}'
  field_extractor = re.compile(field_pattern)

  for line in sys.stdin:
    line = line.strip()
    key_and_json = line.split('\t', 1)
    json = key_and_json[1]
    matches = field_extractor.match(json)
    if matches:
      word = matches.group(2)
      if word:
        print '%s\t%s' % (word, 1)


if __name__ == '__main__':
  main(sys.argv)
