/**
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.hadoop.util;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.http.HttpHost;

/**
 * Factory for creating HttpTransport types.
 */
public class HttpTransportFactory {

  /**
   * Types of HttpTransports the factory can create.
   */
  public enum HttpTransportType {
    APACHE,
    JAVA_NET,
  }

  // Default to javanet implementation.
  public static final HttpTransportType DEFAULT_TRANSPORT_TYPE = HttpTransportType.JAVA_NET;

  /**
   * Utility for getting {@link HttpTransportType}s form names, with default.
   *
   * @param typeName The name of the {@link HttpTransportType} type to return. A default will be
   * used if null or empty.
   * @return The corresponding HttpTransportType.
   * @throws IllegalArgumentException if the name is not an HttpTransportType.
   */
  public static HttpTransportType getTransportTypeOf(@Nullable String typeName) {
    HttpTransportType type = DEFAULT_TRANSPORT_TYPE;
    if (!Strings.isNullOrEmpty(typeName)) {
      try {
        type = HttpTransportType.valueOf(typeName);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid HttpTransport type '%s'. Must be one of %s.", typeName,
                Arrays.toString(HttpTransportType.values())), e);
      }
    }
    return type;
  }

  /**
   * Create an {@link HttpTransport} based on an type class and an optional HTTP proxy.
   *
   * @param type The type of HttpTransport to use.
   * @param proxyAddress The HTTP proxy to use with the transport. Of the form hostname:port. If
   * empty no proxy will be used.
   * @return The resulting HttpTransport.
   * @throws IllegalArgumentException If the proxy address is invalid.
   * @throws IOException If there is an issue connecting to Google's Certification server.
   */
  public static HttpTransport createHttpTransport(
      HttpTransportType type, @Nullable String proxyAddress) throws IOException {
    try {
      URI proxyUri = parseProxyAddress(proxyAddress);
      switch (type) {
        case APACHE:
          HttpHost proxyHost = null;
          if (proxyUri != null) {
            proxyHost = new HttpHost(proxyUri.getHost(), proxyUri.getPort());
          }
          return createApacheHttpTransport(proxyHost);
        case JAVA_NET:
          Proxy proxy = null;
          if (proxyUri != null) {
            proxy = new Proxy(
                Proxy.Type.HTTP, new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()));
          }
          return createNetHttpTransport(proxy);
        default:
          throw new IllegalArgumentException(
              String.format("Invalid HttpTransport type '%s'", type.name()));
      }
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  /**
   * Create an {@link ApacheHttpTransport} for calling Google APIs with an optional HTTP proxy.
   *
   * @param proxy Optional HTTP proxy to use with the transport.
   * @return The resulting HttpTransport.
   * @throws IOException If there is an issue connecting to Google's certification server.
   * @throws GeneralSecurityException If there is a security issue with the keystore.
   */
  public static ApacheHttpTransport createApacheHttpTransport(@Nullable HttpHost proxy)
      throws IOException, GeneralSecurityException {
    ApacheHttpTransport.Builder builder = new ApacheHttpTransport.Builder();
    builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
    builder.setProxy(proxy);
    return builder.build();
  }

  /**
   * Create an {@link NetHttpTransport} for calling Google APIs with an optional HTTP proxy.
   *
   * @param proxy Optional HTTP proxy to use with the transport.
   * @return The resulting HttpTransport.
   * @throws IOException If there is an issue connecting to Google's certification server.
   * @throws GeneralSecurityException If there is a security issue with the keystore.
   */
  public static NetHttpTransport createNetHttpTransport(@Nullable Proxy proxy)
      throws IOException, GeneralSecurityException {
    NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
    builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
    builder.setProxy(proxy);
    return builder.build();
  }

  /**
   * Convenience method equivalent to {@link GoogleNetHttpTransport#newTrustedTransport()}.
   */
  public static HttpTransport newTrustedTransport()
      throws GeneralSecurityException, IOException {
    return createNetHttpTransport(null);
  }

  /**
   * Parse an HTTP proxy from a String address.
   * @param proxyAddress The address of the proxy of the form (https?://)HOST:PORT.
   * @return The URI of the proxy.
   * @throws IllegalArgumentException If the address is invalid.
   */
  @VisibleForTesting
  static URI parseProxyAddress(@Nullable String proxyAddress) {
    if (Strings.isNullOrEmpty(proxyAddress)) {
      return null;
    }
    String uriString = proxyAddress;
    if (!uriString.contains("//")) {
      uriString = "//" + uriString;
    }
    try {
      URI uri = new URI(uriString);
      String scheme = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      if (!Strings.isNullOrEmpty(scheme) && !scheme.matches("https?")) {
        throw new IllegalArgumentException(
            String.format(
                "HTTP proxy address '%s' has invalid scheme '%s'.", proxyAddress, scheme));
      } else if (Strings.isNullOrEmpty(host)) {
        throw new IllegalArgumentException(
            String.format("Proxy address '%s' has no host.", proxyAddress));
      } else if (port == -1) {
        throw new IllegalArgumentException(
            String.format("Proxy address '%s' has no port.", proxyAddress));
      } else if (!uri.equals(new URI(scheme, null, host, port, null, null, null))) {
        throw new IllegalArgumentException(
            String.format("Invalid proxy address '%s'.", proxyAddress));
      }
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid proxy address '%s'.", proxyAddress), e);
    }
  }
}
