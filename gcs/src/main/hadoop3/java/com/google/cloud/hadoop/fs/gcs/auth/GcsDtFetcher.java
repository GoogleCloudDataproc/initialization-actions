/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.hadoop.fs.gcs.auth;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.SCHEME;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtFetcher;
import org.apache.hadoop.security.token.Token;

/**
 * A DT fetcher for GCS. This is a copy-and-paste of {@code org.apache.hadoop.hdfs.HdfsDtFetcher}.
 */
public class GcsDtFetcher implements DtFetcher {

  /** Returns the service name for GCS, which is also a valid URL prefix. */
  @Override
  public Text getServiceName() {
    return new Text(SCHEME);
  }

  @Override
  public boolean isTokenRequired() {
    return UserGroupInformation.isSecurityEnabled();
  }

  /**
   * Returns Token object via FileSystem, null if bad argument.
   *
   * @param conf - a Configuration object used with FileSystem.get()
   * @param creds - a Credentials object to which token(s) will be added
   * @param renewer - the renewer to send with the token request
   * @param url - the URL to which the request is sent
   * @return a Token, or null if fetch fails.
   */
  @Override
  public Token<?> addDelegationTokens(
      Configuration conf, Credentials creds, String renewer, String url) throws Exception {
    if (!url.startsWith(SCHEME)) {
      url = SCHEME + "://" + url;
    }
    FileSystem fs = FileSystem.get(URI.create(url), conf);
    Token<?> token = fs.getDelegationToken(renewer);
    if (token == null) {
      throw new DelegationTokenIOException("Filesystem not generating Delegation Tokens: " + url);
    }
    creds.addToken(token.getService(), token);
    return token;
  }
}
