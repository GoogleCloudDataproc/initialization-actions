/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.gcsio;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * UnitTests for GoogleCloudStorageStrings class.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageStringsTest {
  @Test
  public void testConstructorForLineCoverage() {
    new GoogleCloudStorageStrings();
  }

  @Test
  public void testMatchListPrefixIllegalArguments() {
    String[][] invalidArgs = {
      {"my-prefix", "/", null},
      {"my-prefix", "/", ""},
      // Even if prefix and delimiter are null, the argument-check should throw.
      {null, null, null},
      {null, null, ""},
    };

    for (String[] args : invalidArgs) {
      IllegalArgumentException iae =
          assertThrows(
              IllegalArgumentException.class,
              () -> GoogleCloudStorageStrings.matchListPrefix(args[0], args[1], args[2]));
      assertThat(iae).hasMessageThat().matches(".*objectName.*");
    }
  }

  /**
   * This data structure encompassing a single test case including input args and expected output.
   */
  private static class MatchResultExpectation {
    public final String objectNamePrefix;
    public final String delimiter;
    public final String objectName;
    public String returnValue;

    /**
     * The args which will be passed to GoogleCloudStorageStrings.matchListPrefix.
     */
    public MatchResultExpectation(String objectNamePrefix, String delimiter, String objectName) {
      this.objectNamePrefix = objectNamePrefix;
      this.delimiter = delimiter;
      this.objectName = objectName;
    }

    /**
     * Sets the expected return value.
     */
    public MatchResultExpectation willReturn(String returnValue) {
      this.returnValue = returnValue;
      return this;
    }

    /**
     * Useful formatted String summarizing this expectation.
     */
    @Override
    public String toString() {
      return String.format("Expect '%s' for args (%s, %s, %s)",
          returnValue, objectNamePrefix, delimiter, objectName);
    }
  }

  /**
   * Helper for verifying an array of test-case expectations.
   */
  private void verifyExpectations(MatchResultExpectation[] expectations) {
    for (MatchResultExpectation expectation : expectations) {
      String actualReturn = GoogleCloudStorageStrings.matchListPrefix(
          expectation.objectNamePrefix, expectation.delimiter, expectation.objectName);
      assertWithMessage(
              String.format("Got returnValue '%s' for expectation: %s", actualReturn, expectation))
          .that(actualReturn)
          .isEqualTo(expectation.returnValue);
    }
  }

  @Test
  public void testMatchListPrefixNoPrefixNoDelimiter() {
    MatchResultExpectation[] expectations = {
      // Return value should always be equal to input.
      new MatchResultExpectation(null, null, "/")
          .willReturn("/"),

      new MatchResultExpectation(null, null, "foo/bar")
          .willReturn("foo/bar"),

      new MatchResultExpectation(null, "", "foo/bar")
          .willReturn("foo/bar"),

      new MatchResultExpectation("", "", "foo/bar")
          .willReturn("foo/bar"),
    };

    verifyExpectations(expectations);
  }

  @Test
  public void testMatchListPrefixNoDelimiter() {
    MatchResultExpectation[] expectations = {
      // Match should succeed and return entire input.
      new MatchResultExpectation("foo/bar/baz", null, "foo/bar/baz123")
          .willReturn("foo/bar/baz123"),

      // Exact match doesn't succeed because our GoogleCloudStorage is made to filter it out if
      // no delimiter was provided.
      new MatchResultExpectation("foo/bar/baz", null, "foo/bar/baz")
          .willReturn((String) null),

      // String shorter than the prefix will not succeed.
      new MatchResultExpectation("foo/bar/baz", null, "foo/bar/ba")
          .willReturn((String) null),

      // Since no delimiter was passed, '/' should not cause truncation.
      new MatchResultExpectation("foo/bar/baz", null, "foo/bar/baz/sub")
          .willReturn("foo/bar/baz/sub"),

      // Prefix match where objectName ends with '/'.
      new MatchResultExpectation("foo/bar/baz", null, "foo/bar/baz/")
          .willReturn("foo/bar/baz/"),

      // Exact match where both prefix and objectName end with '/'.
      new MatchResultExpectation("foo/bar/baz/", null, "foo/bar/baz/")
          .willReturn((String) null),
    };

    verifyExpectations(expectations);
  }

  @Test
  public void testMatchListPrefixNoPrefix() {
    MatchResultExpectation[] expectations = {
      new MatchResultExpectation(null, "/", "/")
          .willReturn("/"),

      // Truncates to the first occurrence of the delimiter.
      new MatchResultExpectation(null, "/", "foo/bar")
          .willReturn("foo/"),

      new MatchResultExpectation(null, "/", "foo/bar/")
          .willReturn("foo/"),

      new MatchResultExpectation(null, "/", "foo/")
          .willReturn("foo/"),

      // If the delimiter isn't in the String, it will return unchanged.
      new MatchResultExpectation(null, "/", "foo")
          .willReturn("foo"),

      // "First occurrence" includes index 0.
      new MatchResultExpectation(null, "/", "/foo/bar")
          .willReturn("/"),
    };

    verifyExpectations(expectations);
  }

  @Test
  public void testMatchListPrefixWithPrefixAndDelimiter() {
    MatchResultExpectation[] expectations = {
      new MatchResultExpectation("foo/bar", "/", "foo/bar/baz")
          .willReturn("foo/bar/"),

      // Include some extra characters between the prefix-matched portion and the following
      // delimiter.
      new MatchResultExpectation("foo/bar", "/", "foo/bar123/baz")
          .willReturn("foo/bar123/"),

      // Exact match and ends with delimiter means we return null.
      new MatchResultExpectation("foo/bar/", "/", "foo/bar/")
          .willReturn((String) null),

      // The delimiter-truncation search begins *strictly after* the prefix; if the prefix
      // ends with the delimiter, then truncation occurs at the *next* delimiter after it.
      new MatchResultExpectation("foo/bar/", "/", "foo/bar/baz")
          .willReturn("foo/bar/baz"),

      new MatchResultExpectation("foo/bar/", "/", "foo/bar/baz/bat")
          .willReturn("foo/bar/baz/"),


      // Multi-char delimiters work too.
      new MatchResultExpectation("foo$$bar", "$$", "foo$$bar$$baz")
          .willReturn("foo$$bar$$"),

      new MatchResultExpectation("foo$$bar$$", "$$", "foo$$bar$$baz")
          .willReturn("foo$$bar$$baz"),

      new MatchResultExpectation("foo$$bar$$", "$$", "foo$$bar$$baz$$bat")
          .willReturn("foo$$bar$$baz$$"),
    };

    verifyExpectations(expectations);
  }
}
