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

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createCompositeException;
import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageExceptions.createFileNotFoundException;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GoogleCloudStorageExceptions} class. */
@RunWith(JUnit4.class)
public class GoogleCloudStorageExceptionsTest {
  @Test
  public void testGetFileNotFoundExceptionThrowsWhenBucketNameIsNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> createFileNotFoundException(/* bucketName= */ null, "obj", /* cause= */ null));
  }

  @Test
  public void testGetFileNotFoundExceptionThrowsWhenBucketNameIsEmpty() {
    assertThrows(
        IllegalArgumentException.class,
        () -> createFileNotFoundException("", "obj", /* cause= */ null));
  }

  @Test
  public void testGetFileNotFoundException() {
    assertThat(createFileNotFoundException("bucket", /* objectName= */ null, /* cause= */ null))
        .hasMessageThat()
        .startsWith("Item not found: 'gs://bucket/'.");

    assertThat(createFileNotFoundException("bucket", "", /* cause= */ null))
        .hasMessageThat()
        .startsWith("Item not found: 'gs://bucket/'.");

    assertThat(createFileNotFoundException("bucket", "obj", /* cause= */ null))
        .hasMessageThat()
        .startsWith("Item not found: 'gs://bucket/obj'.");
  }

  @Test
  public void createFileNotFoundException_withCause() {
    IOException cause = new IOException("cause exception");
    FileNotFoundException e = createFileNotFoundException("bucket", "object", cause);
    assertThat(e).hasCauseThat().isSameInstanceAs(cause);
  }

  @Test
  public void testConstructorThrowsWhenInnerExceptionsAreEmpty() {
    List<IOException> emptyList = ImmutableList.of();
    assertThrows(IllegalArgumentException.class, () -> createCompositeException(emptyList));
  }

  @Test
  public void testConstructorThrowsWhenInnerExceptionsAreNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> createCompositeException(/* innerExceptions= */ null));
  }

  @Test
  public void createCompositeException_withSingleInnerException() {
    IOException inner = new IOException("inner exception");
    IOException compositeException = createCompositeException(ImmutableList.of(inner));
    assertThat(inner).isSameInstanceAs(compositeException);
  }

  @Test
  public void createCompositeException_withMultipleInnerExceptions() {
    IOException inner1 = new IOException("inner exception 1");
    IOException inner2 = new IOException("inner exception 2");

    IOException compositeException = createCompositeException(ImmutableList.of(inner1, inner2));

    assertThat(inner1).isNotSameInstanceAs(compositeException);
    assertThat(inner2).isNotSameInstanceAs(compositeException);
    assertThat(compositeException).hasMessageThat().isEqualTo("Multiple IOExceptions.");
    assertThat(compositeException.getSuppressed()).isEqualTo(new Throwable[] {inner1, inner2});
    assertThat(compositeException.getSuppressed()[0]).isSameInstanceAs(inner1);
    assertThat(compositeException.getSuppressed()[1]).isSameInstanceAs(inner2);
  }
}
