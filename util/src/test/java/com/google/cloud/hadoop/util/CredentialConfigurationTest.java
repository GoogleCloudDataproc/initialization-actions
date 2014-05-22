/**
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

package com.google.cloud.hadoop.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;


@RunWith(JUnit4.class)
public class CredentialConfigurationTest {

  public static final List<String> TEST_SCOPES = ImmutableList.of("scope1", "scope2");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void nullCredentialsAreCreatedForTesting() throws IOException, GeneralSecurityException {
    CredentialConfiguration configuration = new CredentialConfiguration();
    configuration.setEnableServiceAccounts(false);
    configuration.setNullCredentialEnabled(true);

    Assert.assertNull(configuration.getCredential(TEST_SCOPES));
  }

  @Test
  public void exceptionIsThrownForNoServiceAccountEmail()
      throws IOException, GeneralSecurityException {

    // No email set, keyfile doesn't exist, but that's OK.
    CredentialConfiguration configuration = new CredentialConfiguration();
    configuration.setServiceAccountKeyFile("aFile");

    expectedException.expect(IllegalStateException.class);
    configuration.getCredential(TEST_SCOPES);
  }

  @Test
  public void exceptionIsThrownForNoCredentialOptions()
      throws IOException, GeneralSecurityException {
    CredentialConfiguration configuration = new CredentialConfiguration();
    configuration.setEnableServiceAccounts(false);

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("No valid credential configuration discovered.");

    configuration.getCredential(TEST_SCOPES);
  }


  @Test
  public void metadataServiceIsUsedByDefault() throws IOException, GeneralSecurityException {
    CredentialFactory mockCredentialFactory = mock(CredentialFactory.class);

    CredentialConfiguration configuration = new CredentialConfiguration();
    configuration.setCredentialFactory(mockCredentialFactory);
    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1)).getCredentialFromMetadataServiceAccount();
    verifyNoMoreInteractions(mockCredentialFactory);
  }

  @Test
  public void p12KeyfileUsedWhenConfigured() throws IOException, GeneralSecurityException  {

    CredentialFactory mockCredentialFactory = mock(CredentialFactory.class);

    CredentialConfiguration configuration = new CredentialConfiguration();
    configuration.setCredentialFactory(mockCredentialFactory);
    configuration.setServiceAccountEmail("foo@example.com");
    configuration.setServiceAccountKeyFile("exampleKeyfile");

    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1)).getCredentialFromPrivateKeyServiceAccount(
        "foo@example.com",
        "exampleKeyfile",
        TEST_SCOPES);
    verifyNoMoreInteractions(mockCredentialFactory);
  }

  @Test
  public void installedAppWorkflowUsedWhenConfigurred()
      throws IOException, GeneralSecurityException  {

    CredentialFactory mockCredentialFactory = mock(CredentialFactory.class);

    CredentialConfiguration configuration = new CredentialConfiguration();
    configuration.setCredentialFactory(mockCredentialFactory);
    configuration.setEnableServiceAccounts(false);
    configuration.setClientSecret("aClientSecret");
    configuration.setClientId("aClientId");
    configuration.setOAuthCredentialFile("aCredentialFile");

    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1)).getCredentialFromFileCredentialStoreForInstalledApp(
        "aClientId", "aClientSecret", "aCredentialFile", TEST_SCOPES);
    verifyNoMoreInteractions(mockCredentialFactory);
  }
}
