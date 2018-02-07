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

import static org.junit.Assert.assertThrows;

import com.google.common.truth.Truth;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UriEncodingPathCodecTest {
  UriEncodingPathCodec codec = new UriEncodingPathCodec();

  @Test
  public void testGetPath_PathEncoding() {
    URI id = codec.getPath("b1", "/path/to/!@#$%&*()_/my/resource", false);

    Truth.assertThat(id.getAuthority()).isEqualTo("b1");
    Truth.assertThat(id.getScheme()).isEqualTo("gs");
    Truth.assertThat(id.getRawSchemeSpecificPart())
        .isEqualTo("//b1/path/to/!@%23$%25&*()_/my/resource");
    Truth.assertThat(id.getPath())
        .isEqualTo("/path/to/!@#$%&*()_/my/resource");
  }

  @Test
  public void testGetPath_BadFragments() {
    URI id = codec.getPath("b1", "/path/to/segment1_#Foo#bar#123", false);

    Truth.assertThat(id.getAuthority()).isEqualTo("b1");
    Truth.assertThat(id.getScheme()).isEqualTo("gs");
    Truth.assertThat(id.getRawSchemeSpecificPart())
        .isEqualTo("//b1/path/to/segment1_%23Foo%23bar%23123");
    Truth.assertThat(id.getPath())
        .isEqualTo("/path/to/segment1_#Foo#bar#123");
  }

  @Test
  public void testGetPath_InvalidObjectName() {
    assertThrows(IllegalArgumentException.class, () -> codec.getPath("b1", "", false));
  }

  @Test
  public void testGetPath_InvalidBucketName() {
    assertThrows(IllegalArgumentException.class, () -> codec.getPath("", "/foo/bar", false));
  }

  @Test
  public void testValidatePathAndGetId_Foo() throws URISyntaxException {
    StorageResourceId id =
        codec.validatePathAndGetId(new URI("gs", "bucketName", "/object/name", null), true);

    Truth.assertThat(id.getBucketName()).isEqualTo("bucketName");
    Truth.assertThat(id.getObjectName()).isEqualTo("object/name");
  }

  @Test
  public void testRoundTrip() {
    String objectName = "foo/!@#$%&*()@#$& %%()/bar";
    StorageResourceId rid1 = new StorageResourceId("bucket1", objectName);
    URI encodedURI = codec.getPath(rid1.getBucketName(), rid1.getObjectName(), true);
    StorageResourceId rid2 = codec.validatePathAndGetId(encodedURI, true);

    Truth.assertThat(rid1).isEqualTo(rid2);

    Truth.assertThat(rid2.getBucketName()).isEqualTo("bucket1");
    Truth.assertThat(rid2.getObjectName()).isEqualTo(objectName);
  }

  @Test
  public void testAuthorityAllowsNonHostCharacters() {
    // Ensure that buckets do not require hostname parsing:
    URI withRegistryBasedAuthority = codec.getPath("foo_bar", "/some/object/name", true);

    Truth.assertThat(withRegistryBasedAuthority.getAuthority()).isEqualTo("foo_bar");
    Truth.assertThat(withRegistryBasedAuthority.getRawAuthority()).isEqualTo("foo_bar");
  }
}
