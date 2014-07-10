/**
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PropertyUtilTest {
  @Test
  public void defaultValueIsReturnedWhenNoFile() {
    Assert.assertEquals("NotFound",
        PropertyUtil.getPropertyOrDefault(getClass(), "NonExistentFile", "testKey", "NotFound"));
  }

  @Test
  public void defaultValueIsReturnedWhenKeyNotFound() {
    Assert.assertEquals("NotFound",
        PropertyUtil.getPropertyOrDefault(getClass(), "test.properties", "testKey2", "NotFound"));
  }

  @Test
  public void valueIsReturnedForFoundKeyAndFile() {
    Assert.assertEquals("testValue",
        PropertyUtil.getPropertyOrDefault(getClass(), "test.properties", "testKey", "NotFound"));
  }

  @Test
  public void valueWithWhitespaceIsReadProperly() {
    Assert.assertEquals("test value with whitespace",
        PropertyUtil.getPropertyOrDefault(
            getClass(),
            "test.properties",
            "whitespaceKey",
            "NotFound"));
  }

  @Test
  public void valueWithEscapedCharactersIsReadUnescaped() {
    Assert.assertEquals("http://www.example.com",
        PropertyUtil.getPropertyOrDefault(
            getClass(),
            "test.properties",
            "escapedValueKey",
            "NotFound"));
  }

  @Test
  public void keysAfterCommentsAreFound() {
    Assert.assertEquals("postCommentValue",
        PropertyUtil.getPropertyOrDefault(
            getClass(),
            "test.properties",
            "postCommentKey",
            "NotFound"));
  }
}
