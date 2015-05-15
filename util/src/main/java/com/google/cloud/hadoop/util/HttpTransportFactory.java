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
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.hadoop.util;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.base.Strings;

import org.apache.http.HttpHost;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.util.Arrays;

import javax.annotation.Nullable;

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
                Arrays.toString(HttpTransportType.values())),
            e);
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
   * @throws IOException If there is an issue connecting to Google's Certification server.
   * @throws GeneralSecurityException If there is a security issue with the keystore.
   */
  public static HttpTransport createHttpTransport(
      HttpTransportType type, @Nullable String proxyAddress) throws IOException {
    HttpHost proxyHost = null;
    if (!Strings.isNullOrEmpty(proxyAddress)) {
      proxyHost = new HttpHost(proxyAddress);
    }
    try {
      switch (type) {
        case APACHE:
          return createApacheHttpTransport(proxyHost);
        case JAVA_NET:
          Proxy proxy = null;
          if (proxyHost != null) {
            proxy = new Proxy(
                Proxy.Type.HTTP,
                new InetSocketAddress(proxyHost.getHostName(), proxyHost.getPort()));
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
}
