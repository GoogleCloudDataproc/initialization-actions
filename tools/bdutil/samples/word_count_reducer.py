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
"""Reducer for use with hadoop-streaming word-count example.

Reads each line of input, sums the counts for each word,
outputs a line with word and total count for each word.
The input is assumed to be sorted by word.
"""

from __future__ import print_function

import re
import sys

current_word = None
current_count = 0
output_json = False


def print_word_and_count(word, count):
  word = re.sub('"', "'", word)   # replace double-quotes with single-quotes
  if output_json:
    print('0\t{"Word": "%s", "Count": %d}' % (word, count))
    # When streaming out to BigQuery, this key (0 here) is ignored.
  else:
    print('%s\t%s' % (word, count))


def next_word(word, count):
  global current_word, current_count
  if current_word:
    print_word_and_count(current_word, current_count)
  current_word = word
  current_count = count


def main(args):
  global current_count
  global output_json

  if len(args) > 1:
    if args[1] == '--output_json':
      output_json = True
    else:
      print("Unknown command line option '%s'" % args[1], file=sys.stderr)
      sys.exit(2)

  for line in sys.stdin:
    line = line.strip()
    word, count_string = line.split('\t', 1)

    try:
      count = int(count_string)
    except ValueError:
      continue    # ignore lines that are not formatted correctly

    if word == current_word:
      current_count += count
    else:
      next_word(word, count)

  next_word(None, 0)

if __name__ == '__main__':
  main(sys.argv)
