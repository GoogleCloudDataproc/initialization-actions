/*
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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpTransport;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CredentialConfigurationTest {

  public static final ImmutableList<String> TEST_SCOPES = ImmutableList.of("scope1", "scope2");

  private final CredentialFactory mockCredentialFactory = mock(CredentialFactory.class);
  private final HttpTransport mockTransport = mock(HttpTransport.class);
  private CredentialConfiguration configuration;

  @Before
  public void setUp() {
    configuration = new CredentialConfiguration();
    configuration.setCredentialFactory(mockCredentialFactory);
    configuration.setTransport(mockTransport);
  }

  @Test
  public void nullCredentialsAreCreatedForTesting() throws IOException, GeneralSecurityException {
    configuration.setEnableServiceAccounts(false);
    configuration.setNullCredentialEnabled(true);

    assertThat(configuration.getCredential(TEST_SCOPES)).isNull();
  }

  @Test
  public void exceptionIsThrownForNoServiceAccountEmail()
      throws IOException, GeneralSecurityException {
    // No email set, keyfile doesn't exist, but that's OK.
    configuration.setServiceAccountKeyFile("aFile");

    assertThrows(IllegalStateException.class, () -> configuration.getCredential(TEST_SCOPES));
  }

  @Test
  public void exceptionIsThrownForNoCredentialOptions()
      throws IOException, GeneralSecurityException {
    configuration.setEnableServiceAccounts(false);

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> configuration.getCredential(TEST_SCOPES));
    assertThat(thrown).hasMessageThat().contains("No valid credential configuration discovered.");
  }

  @Test
  public void metadataServiceIsUsedByDefault() throws IOException, GeneralSecurityException {
    configuration.getCredential(TEST_SCOPES);
    when(mockCredentialFactory.hasApplicationDefaultCredentialsConfigured()).thenReturn(false);

    verify(mockCredentialFactory, times(1)).getCredentialFromMetadataServiceAccount();
  }

  @Test
  public void applicationDefaultServiceAccountWhenConfigured()
      throws IOException, GeneralSecurityException {
    when(mockCredentialFactory.hasApplicationDefaultCredentialsConfigured()).thenReturn(true);

    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1))
        .getApplicationDefaultCredentials(TEST_SCOPES, mockTransport);
  }

  @Test
  public void p12KeyfileUsedWhenConfigured() throws IOException, GeneralSecurityException {
    configuration.setServiceAccountEmail("foo@example.com");
    configuration.setServiceAccountKeyFile("exampleKeyfile");

    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1))
        .getCredentialFromPrivateKeyServiceAccount(
            "foo@example.com", "exampleKeyfile", TEST_SCOPES, mockTransport);
  }

  @Test
  public void jsonKeyFileUsedWhenConfigured() throws IOException, GeneralSecurityException {
    configuration.setServiceAccountJsonKeyFile("jsonExampleKeyFile");

    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1))
        .getCredentialFromJsonKeyFile("jsonExampleKeyFile", TEST_SCOPES, mockTransport);
  }

  @Test
  public void configurationSAUsedWhenConfigured() throws IOException, GeneralSecurityException {
    configuration.setServiceAccountEmail("foo@example.com");
    configuration.setServiceAccountPrivateKeyId("privateKeyId");
    configuration.setServiceAccountPrivateKey("privateKey");

    configuration.getCredential(TEST_SCOPES);
    verify(mockCredentialFactory, times(1))
        .getCredentialsFromSAParameters(
            "privateKeyId", "privateKey", "foo@example.com", TEST_SCOPES, mockTransport);
  }

  @Test
  public void installedAppWorkflowUsedWhenConfigurred()
      throws IOException, GeneralSecurityException  {
    configuration.setEnableServiceAccounts(false);
    configuration.setClientSecret("aClientSecret");
    configuration.setClientId("aClientId");
    configuration.setOAuthCredentialFile("aCredentialFile");

    configuration.getCredential(TEST_SCOPES);

    verify(mockCredentialFactory, times(1))
        .getCredentialFromFileCredentialStoreForInstalledApp(
            "aClientId", "aClientSecret", "aCredentialFile", TEST_SCOPES, mockTransport);
  }
}
