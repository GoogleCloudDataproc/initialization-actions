/**
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or typeied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import static org.junit.Assert.assertEquals;

import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HttpTransportFactoryTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetTransportTypeOfDefault() throws Exception {
    HttpTransportFactory.HttpTransportType type = HttpTransportFactory.getTransportTypeOf(null);
    assertEquals(HttpTransportType.JAVA_NET, type);
    type = HttpTransportFactory.getTransportTypeOf("");
    assertEquals(HttpTransportType.JAVA_NET, type);
  }

  @Test
  public void testGetTransportTypeOf() throws Exception {
    HttpTransportFactory.HttpTransportType type = HttpTransportFactory.getTransportTypeOf(
        "JAVA_NET");
    assertEquals(HttpTransportFactory.HttpTransportType.JAVA_NET, type);
    type = HttpTransportFactory.getTransportTypeOf("APACHE");
    assertEquals(HttpTransportType.APACHE, type);
  }

  @Test
  public void testGetTransportTypeOfException() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        String.format(
            "Invalid HttpTransport type 'com.google.api.client.http.apache.ApacheHttpTransport'."
                + " Must be one of [APACHE, JAVA_NET]."));
    HttpTransportFactory.getTransportTypeOf(
        "com.google.api.client.http.apache.ApacheHttpTransport");
  }
}