/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.hadoop.fs.gcs.auth;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/**
 * General IOException for Delegation Token issues. Includes recommended error strings, which can be
 * used in tests when looking for specific errors.
 */
public class DelegationTokenIOException extends IOException {

  /**
   * Version number for serialization. See more info at:
   * https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/io/Serializable.html
   */
  private static final long serialVersionUID = 5431764092856006083L;

  public static DelegationTokenIOException wrongTokenType(
      Class expectedClass, DelegationTokenIdentifier identifier) {
    return new DelegationTokenIOException(
        String.format(
            "Delegation token type is incorrect;"
                + " expected a token identifier of type %s but got %s and kind %s",
            expectedClass, identifier.getClass(), identifier.getKind()));
  }

  public static DelegationTokenIOException tokenMismatch(
      Text service, Text expectedKind, Text actualKind) {
    return new DelegationTokenIOException(
        String.format(
            "Token mismatch: expected token for %s of type %s but got a token of type %s",
            service, expectedKind, actualKind));
  }

  public DelegationTokenIOException(String message) {
    super(message);
  }

  public DelegationTokenIOException(String message, final Throwable cause) {
    super(message, cause);
  }
}
