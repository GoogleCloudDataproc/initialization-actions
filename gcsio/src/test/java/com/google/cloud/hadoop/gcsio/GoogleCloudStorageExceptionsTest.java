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
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for GoogleCloudStorageExceptions class.
 */
@RunWith(JUnit4.class)
public class GoogleCloudStorageExceptionsTest {
  @Test
  public void testGetFileNotFoundExceptionThrowsWhenBucketNameIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleCloudStorageExceptions.getFileNotFoundException(null, "obj"));
  }

  @Test
  public void testGetFileNotFoundExceptionThrowsWhenBucketNameIsEmpty() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleCloudStorageExceptions.getFileNotFoundException("", "obj"));
  }

  /** Validates getFileNotFoundException(). */
  @Test
  public void testGetFileNotFoundException() {
    // objectName is null or empty
    FileNotFoundException e = GoogleCloudStorageExceptions.getFileNotFoundException("bucket", null);
    assertThat(e).hasMessageThat().startsWith("Item not found: 'gs://bucket/'.");
    assertThat(GoogleCloudStorageExceptions.getFileNotFoundException("bucket", ""))
        .hasMessageThat()
        .isEqualTo(e.getMessage());

    assertThat(GoogleCloudStorageExceptions.getFileNotFoundException("bucket", "obj"))
        .hasMessageThat()
        .startsWith("Item not found: 'gs://bucket/obj'.");
  }

  @Test
  public void testConstructorThrowsWhenInnerExceptionsAreEmpty() {
    List<IOException> emptyList = Lists.newArrayList(new IOException[0]);
    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleCloudStorageExceptions.createCompositeException(emptyList));
  }

  @Test
  public void testConstructorThrowsWhenInnerExceptionsAreNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GoogleCloudStorageExceptions.createCompositeException(null));
  }

  /**
   * Validates createCompositeException().
   */
  @Test
  public void testCreateCompositeException() {
    IOException compositeException;

    // Exactly 1 inner exception should be returned unwrapped.
    IOException inner1 = new IOException("inner1");
    compositeException =
        GoogleCloudStorageExceptions.createCompositeException(ImmutableList.of(inner1));
    assertThat(inner1 == compositeException).isTrue();

    // More than 1 inner exceptions should be wrapped.
    IOException inner2 = new IOException("inner2");
    compositeException =
        GoogleCloudStorageExceptions.createCompositeException(ImmutableList.of(inner1, inner2));
    assertThat(inner1 == compositeException).isFalse();
    assertThat(inner2 == compositeException).isFalse();
    assertThat(compositeException).hasMessageThat().isEqualTo("Multiple IOExceptions.");
  }

  /**
   * Provides coverage for default constructor. No real validation is performed.
   */
  @Test
  public void testCoverDefaultConstructor() {
    new GoogleCloudStorageExceptions();
  }
}
