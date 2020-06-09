package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.jsonDataResponse;
import static com.google.cloud.hadoop.util.testing.MockHttpTransportHelper.mockTransport;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.iamcredentials.v1.model.GenerateAccessTokenResponse;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link GoogleCredentialWithIamAccessToken}. */
@RunWith(JUnit4.class)
public class GoogleCredentialWithIamAccessTokenTest {

  private static final String TEST_ACCESS_TOKEN = "test.token";
  public static final long TEST_TIME_MILLISECONDS = 2000L;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testCreateCredentialFromIamAccessToken() throws IOException {
    GenerateAccessTokenResponse accessTokenRes = new GenerateAccessTokenResponse();
    accessTokenRes.setAccessToken(TEST_ACCESS_TOKEN);
    // 1970-01-01T00:00:02Z is equal to 2000 milliseconds since Epoch time.
    accessTokenRes.setExpireTime("1970-01-01T00:00:02Z");
    MockHttpTransport transport = mockTransport(jsonDataResponse(accessTokenRes));

    GoogleCredential credential =
        new GoogleCredentialWithIamAccessToken(
            transport, ignore -> {}, "test-service-account", CredentialFactory.GCS_SCOPES);

    assertThat(credential.getAccessToken()).isEqualTo(TEST_ACCESS_TOKEN);
    assertThat(credential.getExpirationTimeMilliseconds()).isEqualTo(TEST_TIME_MILLISECONDS);
  }

  @Test
  public void testCreateCredentialFromIamAccessTokenWithoutExpirationTime() throws IOException {
    GenerateAccessTokenResponse accessTokenRes = new GenerateAccessTokenResponse();
    accessTokenRes.setAccessToken(TEST_ACCESS_TOKEN);
    MockHttpTransport transport = mockTransport(jsonDataResponse(accessTokenRes));

    GoogleCredential credential =
        new GoogleCredentialWithIamAccessToken(
            transport, ignore -> {}, "test-service-account", CredentialFactory.GCS_SCOPES);

    assertThat(credential.getAccessToken()).isEqualTo(TEST_ACCESS_TOKEN);
  }

  @Test
  public void testCreateCredentialFromIamAccessTokenThrowsExceptionForNullToken()
      throws IOException {
    GenerateAccessTokenResponse accessTokenRes = new GenerateAccessTokenResponse();
    // 1970-01-01T00:00:02Z is equal to 2000 milliseconds since Epoch time.
    accessTokenRes.setExpireTime("1970-01-01T00:00:02Z");
    accessTokenRes.setAccessToken(null);
    MockHttpTransport transport = mockTransport(jsonDataResponse(accessTokenRes));

    assertThrows(
        NullPointerException.class,
        () ->
            new GoogleCredentialWithIamAccessToken(
                transport, ignore -> {}, "test-service-account", CredentialFactory.GCS_SCOPES));
  }
}
