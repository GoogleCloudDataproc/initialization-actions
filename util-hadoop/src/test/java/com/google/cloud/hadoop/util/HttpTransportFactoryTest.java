/*
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.hadoop.util.HttpTransportFactory.HttpTransportType;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HttpTransportFactoryTest {
  @Test
  public void testGetTransportTypeOfDefault() throws Exception {
    HttpTransportFactory.HttpTransportType type = HttpTransportFactory.getTransportTypeOf(null);
    assertThat(type).isEqualTo(HttpTransportType.JAVA_NET);
    type = HttpTransportFactory.getTransportTypeOf("");
    assertThat(type).isEqualTo(HttpTransportType.JAVA_NET);
  }

  @Test
  public void testGetTransportTypeOf() throws Exception {
    HttpTransportFactory.HttpTransportType type = HttpTransportFactory.getTransportTypeOf(
        "JAVA_NET");
    assertThat(type).isEqualTo(HttpTransportFactory.HttpTransportType.JAVA_NET);
    type = HttpTransportFactory.getTransportTypeOf("APACHE");
    assertThat(type).isEqualTo(HttpTransportType.APACHE);
  }

  @Test
  public void testGetTransportTypeOfException() throws Exception {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                HttpTransportFactory.getTransportTypeOf(
                    "com.google.api.client.http.apache.ApacheHttpTransport"));
    assertThat(thrown)
        .hasMessageThat()
        .contains(
            "Invalid HttpTransport type 'com.google.api.client.http.apache.ApacheHttpTransport'."
                + " Must be one of [APACHE, JAVA_NET].");
  }

  @Test
  public void testParseProxyAddress() throws Exception {
    String address = "foo-host:1234";
    URI expectedUri = getURI(null, "foo-host", 1234);
    URI uri = HttpTransportFactory.parseProxyAddress(address);
    assertThat(uri).isEqualTo(expectedUri);
  }

  @Test
  public void testParseProxyAddressHttp() throws Exception {
    String address = "http://foo-host:1234";
    URI expectedUri = getURI("http", "foo-host", 1234);
    URI uri = HttpTransportFactory.parseProxyAddress(address);
    assertThat(uri).isEqualTo(expectedUri);
  }

  @Test
  public void testParseProxyAddressHttps() throws Exception {
    String address = "https://foo-host:1234";
    URI expectedUri = getURI("https", "foo-host", 1234);
    URI uri = HttpTransportFactory.parseProxyAddress(address);
    assertThat(uri).isEqualTo(expectedUri);
  }

  @Test
  public void testParseProxyAddressInvalidScheme() throws Exception {
    String address = "socks5://foo-host:1234";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown)
        .hasMessageThat()
        .contains("HTTP proxy address 'socks5://foo-host:1234' has invalid scheme 'socks5'.");
  }

  @Test
  public void testParseProxyAddressNoHost() throws Exception {
    String address = ":1234";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown).hasMessageThat().contains("Proxy address ':1234' has no host.");
  }

  @Test
  public void testParseProxyAddressNoPort() throws Exception {
    String address = "foo-host";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown).hasMessageThat().contains("Proxy address 'foo-host' has no port.");
  }

  @Test
  public void testParseProxyAddressInvalidSyntax() throws Exception {
    String address = "foo-host-with-illegal-char^:1234";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Invalid proxy address 'foo-host-with-illegal-char^:1234'.");
  }

  @Test
  public void testParseProxyAddressWithPath() throws Exception {
    String address = "foo-host:1234/some/path";

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> HttpTransportFactory.parseProxyAddress(address));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Invalid proxy address 'foo-host:1234/some/path'.");
  }

  private static URI getURI(String scheme, String host, int port) throws URISyntaxException {
    return new URI(scheme, null, host, port, null, null, null);
  }
}
