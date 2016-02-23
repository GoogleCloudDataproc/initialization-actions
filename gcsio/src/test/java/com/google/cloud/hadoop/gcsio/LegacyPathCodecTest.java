/**
 * Copyright 2016 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.gcsio;


import com.google.common.truth.Truth;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.URI;
import java.net.URISyntaxException;

@RunWith(JUnit4.class)
public class LegacyPathCodecTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  LegacyPathCodec codec = new LegacyPathCodec();

  @Test
  public void testGetPath_AlphanumericCharacters() {
    URI id = codec.getPath("b1", "/path/to/my/resource", false);

    Truth.assertThat(id.getAuthority()).isEqualTo("b1");
    Truth.assertThat(id.getScheme()).isEqualTo("gs");
    Truth.assertThat(id.getRawSchemeSpecificPart())
        .isEqualTo("//b1/path/to/my/resource");
    Truth.assertThat(id.getPath())
        .isEqualTo("/path/to/my/resource");
  }

  @Test
  public void testGetPath_PathEncoding() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid bucket name (b1) or object name (path/to/!@#$%&*()_/my/resource)");
    codec.getPath("b1", "/path/to/!@#$%&*()_/my/resource", false);
  }

  @Test
  public void testGetPath_BadFragments() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "Invalid bucket name (b1) or object name (path/to/segment1_#Foo#bar#123)");

    codec.getPath("b1", "path/to/segment1_#Foo#bar#123", false);
  }

  @Test
  public void testGetPath_InvalidObjectName() {
    expectedException.expect(IllegalArgumentException.class);

    codec.getPath("b1", "", false);
  }

  @Test
  public void testGetPath_InvalidBucketName() {
    expectedException.expect(IllegalArgumentException.class);

    codec.getPath("", "/foo/bar", false);
  }

  @Test
  public void testValidatePathAndGetId_ValidObject() throws URISyntaxException {
    StorageResourceId id =
        codec.validatePathAndGetId(new URI("gs", "bucketName", "/object/name", null), true);

    Truth.assertThat(id.getBucketName()).isEqualTo("bucketName");
    Truth.assertThat(id.getObjectName()).isEqualTo("object/name");
  }

  @Test
  public void testValidatePathAndGetId_SpecialCharacterObjectName() throws URISyntaxException {
    StorageResourceId id =
        codec.validatePathAndGetId(
            new URI("gs", "bucketName", "/path/!@#$%^&*()_to/obj", null), true);

    Truth.assertThat(id.getBucketName()).isEqualTo("bucketName");
    // The URI constructor above will percent escape characters that are not valid for a URI
    // path. The legacy codec will preserve this percent escaping.
    Truth.assertThat(id.getObjectName()).isEqualTo("path/!@%23$%25%5E&*()_to/obj");
  }

  @Test
  public void testRoundTrip() throws URISyntaxException {
    // This codec expects input objects to be encoded and it
    // will throw when constructing a URI otherwise.
    String objectName = "path/!@%23$%25%5E&*()_to/obj";

    StorageResourceId rid1 = new StorageResourceId("bucket1", objectName);
    URI encodedURI = codec.getPath(rid1.getBucketName(), rid1.getObjectName(), true);
    StorageResourceId rid2 = codec.validatePathAndGetId(encodedURI, true);

    Truth.assertThat(rid1).isEqualTo(rid2);

    Truth.assertThat(rid2.getBucketName()).isEqualTo("bucket1");
    Truth.assertThat(rid2.getObjectName()).isEqualTo(objectName);
  }
}
