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

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.common.flogger.GoogleLogger;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/** Manages delegation tokens for files system */
public class GcsDelegationTokens {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private GoogleHadoopFileSystemBase fileSystem;

  /**
   * User who owns this FS; fixed at instantiation time, so that in calls to getDelegationToken()
   * and similar, this user is the one whose credentials are involved.
   */
  private final UserGroupInformation user;

  private Text service;

  /** Dynamically loaded token binding; lifecycle matches this object. */
  private AbstractDelegationTokenBinding tokenBinding;

  private AccessTokenProvider accessTokenProvider = null;

  /** Active Delegation token. */
  private Token<DelegationTokenIdentifier> boundDT = null;

  public GcsDelegationTokens() throws IOException {
    user = UserGroupInformation.getCurrentUser();
  }

  public void init(Configuration conf) {
    String tokenBindingImpl = conf.get(DELEGATION_TOKEN_BINDING_CLASS.getKey());

    checkState(tokenBindingImpl != null, "Delegation Tokens are not configured");

    try {
      Class bindingClass = Class.forName(tokenBindingImpl);
      AbstractDelegationTokenBinding binding =
          (AbstractDelegationTokenBinding) bindingClass.newInstance();
      binding.bindToFileSystem(fileSystem, getService());
      tokenBinding = binding;
      logger.atFine().log(
          "Filesystem %s is using delegation tokens of kind %s",
          getService(), tokenBinding.getKind());
      bindToAnyDelegationToken();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Text getService() {
    return service;
  }

  public AccessTokenProvider getAccessTokenProvider() {
    return accessTokenProvider;
  }

  /**
   * Perform the unbonded deployment operations. Create the GCP credential provider chain to use
   * when talking to GCP when there is no delegation token to work with. authenticating this client
   * with GCP services, and saves it to {@link #accessTokenProvider}
   *
   * @throws IOException any failure.
   */
  public AccessTokenProvider deployUnbonded() throws IOException {
    checkState(!isBoundToDT(), "Already Bound to a delegation token");
    logger.atFine().log("No delegation tokens present: using direct authentication");
    accessTokenProvider = tokenBinding.deployUnbonded();
    return accessTokenProvider;
  }

  /**
   * Attempt to bind to any existing DT, including unmarshalling its contents and creating the GCP
   * credential provider used to authenticate the client.
   *
   * <p>If successful:
   *
   * <ol>
   *   <li>{@link #boundDT} is set to the retrieved token.
   *   <li>{@link #accessTokenProvider} is set to the credential provider(s) returned by the token
   *       binding.
   * </ol>
   *
   * If unsuccessful, {@link #deployUnbonded()} is called for the unbonded codepath instead, which
   * will set {@link #accessTokenProvider} to its value.
   *
   * <p>This means after this call (and only after) the token operations can be invoked.
   *
   * @throws IOException selection/extraction/validation failure.
   */
  public void bindToAnyDelegationToken() throws IOException {
    validateAccessTokenProvider();
    Token<DelegationTokenIdentifier> token = selectTokenFromFsOwner();
    if (token != null) {
      bindToDelegationToken(token);
    } else {
      deployUnbonded();
    }
    if (accessTokenProvider == null) {
      throw new DelegationTokenIOException(
          "No AccessTokenProvider created by Delegation Token Binding " + tokenBinding.getKind());
    }
  }

  /**
   * Find a token for the FS user and service name.
   *
   * @return the token, or null if one cannot be found.
   * @throws IOException on a failure to unmarshall the token.
   */
  public Token<DelegationTokenIdentifier> selectTokenFromFsOwner() throws IOException {
    return lookupToken(user.getCredentials(), service, tokenBinding.getKind());
  }

  /**
   * Bind to the filesystem. Subclasses can use this to perform their own binding operations - but
   * they must always call their superclass implementation. This <i>Must</i> be called before
   * calling {@code init()}.
   *
   * <p><b>Important:</b> This binding will happen during FileSystem.initialize(); the FS is not
   * live for actual use and will not yet have interacted with GCS services.
   *
   * @param fs owning FS.
   * @throws IOException failure.
   */
  public void bindToFileSystem(GoogleHadoopFileSystemBase fs, Text service) throws IOException {
    this.service = requireNonNull(service);
    this.fileSystem = requireNonNull(fs);
  }

  /**
   * Bind to a delegation token retrieved for this filesystem. Extract the secrets from the token
   * and set internal fields to the values.
   *
   * <ol>
   *   <li>{@link #boundDT} is set to {@code token}.
   *   <li>{@link #accessTokenProvider} is set to the credential provider(s) returned by the token
   *       binding.
   * </ol>
   *
   * @param token token to decode and bind to.
   * @throws IOException selection/extraction/validation failure.
   */
  public void bindToDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    validateAccessTokenProvider();
    boundDT = token;
    DelegationTokenIdentifier dti = extractIdentifier(token);
    logger.atInfo().log("Using delegation token %s", dti);
    // extract the credential providers.
    accessTokenProvider = tokenBinding.bindToTokenIdentifier(dti);
  }

  /**
   * Predicate: is there a bound DT?
   *
   * @return true if there's a value in {@link #boundDT}.
   */
  public boolean isBoundToDT() {
    return (boundDT != null);
  }

  /**
   * Get any bound DT.
   *
   * @return a delegation token if this instance was bound to it.
   */
  public Token<DelegationTokenIdentifier> getBoundDT() {
    return boundDT;
  }

  /**
   * Get any bound DT or create a new one.
   *
   * @return a delegation token.
   * @throws IOException if one cannot be created
   */
  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public Token<DelegationTokenIdentifier> getBoundOrNewDT(String renewer) throws IOException {
    logger.atFine().log("Delegation token requested");
    if (isBoundToDT()) {
      // the FS was created on startup with a token, so return it.
      logger.atFine().log("Returning current token");
      return getBoundDT();
    }

    // not bound to a token, so create a new one.
    // issued DTs are not cached so that long-lived filesystems can
    // reliably issue session/role tokens.
    return tokenBinding.createDelegationToken(renewer);
  }

  /**
   * From a token, get the session token identifier.
   *
   * @param token token to process
   * @return the session token identifier
   * @throws IOException failure to validate/read data encoded in identifier.
   * @throws IllegalArgumentException if the token isn't an GCP session token
   */
  public static DelegationTokenIdentifier extractIdentifier(
      final Token<? extends DelegationTokenIdentifier> token) throws IOException {
    checkArgument(token != null, "null token");
    DelegationTokenIdentifier identifier;
    // harden up decode beyond what Token does itself
    try {
      identifier = token.decodeIdentifier();
    } catch (RuntimeException e) {
      Throwable cause = e.getCause();
      if (cause != null) {
        // its a wrapping around class instantiation.
        throw new DelegationTokenIOException("Decoding GCS token " + cause, cause);
      }
      throw e;
    }
    if (identifier == null) {
      throw new DelegationTokenIOException("Failed to unmarshall token " + token);
    }
    return identifier;
  }

  /**
   * Look up a token from the credentials, verify it is of the correct kind.
   *
   * @param credentials credentials to look up.
   * @param service service name
   * @param kind token kind to look for
   * @return the token or null if no suitable token was found
   * @throws DelegationTokenIOException wrong token kind found
   */
  private static Token<DelegationTokenIdentifier> lookupToken(
      Credentials credentials, Text service, Text kind) throws DelegationTokenIOException {
    logger.atFine().log("Looking for token for service %s in credentials", service);
    Token<?> token = credentials.getToken(service);
    if (token != null) {
      Text tokenKind = token.getKind();
      logger.atFine().log("Found token of kind %s", tokenKind);
      if (kind.equals(tokenKind)) {
        // The OAuth implementation catches and logs here; this one throws the failure up.
        return (Token<DelegationTokenIdentifier>) token;
      }

      // There's a token for this service, but it's not the right DT kind
      throw DelegationTokenIOException.tokenMismatch(service, kind, tokenKind);
    }
    // A token for the service was not found
    logger.atFine().log("No token found for %s", service);
    return null;
  }

  private void validateAccessTokenProvider() {
    checkState(
        accessTokenProvider == null, "GCP Delegation tokens has already been bound/deployed");
  }
}
