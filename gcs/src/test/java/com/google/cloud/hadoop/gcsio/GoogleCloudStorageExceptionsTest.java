/**
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

import com.google.cloud.hadoop.testing.ExceptionUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Unit tests for GoogleCloudStorageExceptions class.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageExceptionsTest {
  /**
   * Validates getFileNotFoundException().
   */
  @Test
  public void testGetFileNotFoundException() {
    FileNotFoundException e;
    FileNotFoundException e2;

    // Verify that null bucketName throws IllegalArgumentException.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        GoogleCloudStorageExceptions.class, "getFileNotFoundException", String.class, "obj");

    // Verify that empty bucketName throws IllegalArgumentException.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        GoogleCloudStorageExceptions.class, "getFileNotFoundException", "", "obj");

    // objectName is null or empty
    e = GoogleCloudStorageExceptions.getFileNotFoundException("bucket", null);
    e2 = GoogleCloudStorageExceptions.getFileNotFoundException("bucket", "");
    Assert.assertEquals("Item not found: bucket/", e.getMessage());
    Assert.assertEquals(e.getMessage(), e2.getMessage());

    e = GoogleCloudStorageExceptions.getFileNotFoundException("bucket", "obj");
    Assert.assertEquals("Item not found: bucket/obj", e.getMessage());
  }

  /**
   * Validates createCompositeException().
   */
  @Test
  public void testCreateCompositeException() {

    // Verify that empty innerExceptions throws IllegalArgumentException.
    List<IOException> emptyList = Lists.newArrayList(new IOException[0]);
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        GoogleCloudStorageExceptions.class, "createCompositeException", emptyList);

    // Verify that null innerExceptions throws IllegalArgumentException.
    ExceptionUtil.checkThrows(IllegalArgumentException.class,
        GoogleCloudStorageExceptions.class, "createCompositeException", List.class);

    IOException compositeException;

    // Exactly 1 inner exception should be returned unwrapped.
    IOException inner1 = new IOException("inner1");
    compositeException =
        GoogleCloudStorageExceptions.createCompositeException(ImmutableList.of(inner1));
    Assert.assertTrue(inner1 == compositeException);

    // More than 1 inner exceptions should be wrapped.
    IOException inner2 = new IOException("inner2");
    compositeException =
        GoogleCloudStorageExceptions.createCompositeException(ImmutableList.of(inner1, inner2));
    Assert.assertFalse(inner1 == compositeException);
    Assert.assertFalse(inner2 == compositeException);
    Assert.assertEquals("Multiple IOExceptions.", compositeException.getMessage());
  }

  /**
   * Validates wrapException().
   */
  @Test
  public void testWrapException() {
    IOException wrapped;
    IOException inner1 = new IOException("inner1");
    String message = "I am wrapped";
    wrapped = GoogleCloudStorageExceptions.wrapException(inner1, message, "bucket", "object");
    Assert.assertTrue(wrapped.getMessage().startsWith(message));
    Assert.assertEquals(inner1, wrapped.getCause());
  }

  /**
   * Provides coverage for default constructor. No real validation is performed.
   */
  @Test
  public void testCoverDefaultConstructor() {
    new GoogleCloudStorageExceptions();
  }
}
