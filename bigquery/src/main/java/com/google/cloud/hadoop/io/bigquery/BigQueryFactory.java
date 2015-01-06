package com.google.cloud.hadoop.io.bigquery;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.hadoop.util.HadoopCredentialConfiguration;
import com.google.cloud.hadoop.util.LogUtil;
import com.google.cloud.hadoop.util.PropertyUtil;
import com.google.cloud.hadoop.util.RetryHttpInitializer;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class to get BigQuery from environment credentials.
 */
public class BigQueryFactory {

  public static final String BIGQUERY_CONFIG_PREFIX = "mapred.bq";
  // BigQuery scopes for OAUTH.
  public static final List<String> BIGQUERY_OAUTH_SCOPES =
      Arrays.asList("https://www.googleapis.com/auth/bigquery");

  // Service account environment variable name for BigQuery Authentication.
  public static final String BIGQUERY_SERVICE_ACCOUNT = "BIGQUERY_SERVICE_ACCOUNT";

  // Environment variable name for variable specifying path of private key file for BigQuery
  // Authentication.
  public static final String BIGQUERY_PRIVATE_KEY_FILE = "BIGQUERY_PRIVATE_KEY_FILE";

  // Logger.
  protected static final LogUtil log = new LogUtil(BigQueryFactory.class);

  // A resource file containing bigquery related build properties.
  public static final String PROPERTIES_FILE = "bigquery.properties";

  // The key in the PROPERTIES_FILE that contains the version built.
  public static final String VERSION_PROPERTY = "bigquery.connector.version";

  // The version returned when one cannot be found in properties.
  public static final String UNKNOWN_VERSION = "0.0.0";

  // Current version.
  public static final String VERSION;

  // Identifies this version of the Hadoop BigQuery Connector library.
  public static final String BQC_ID;

  static {
    VERSION = PropertyUtil.getPropertyOrDefault(
        BigQueryFactory.class, PROPERTIES_FILE, VERSION_PROPERTY, UNKNOWN_VERSION);
    log.info("Bigquery connector version %s", VERSION);
    BQC_ID = String.format("Hadoop BigQuery Connector/%s", VERSION);
  }

  // Objects for handling HTTP transport and JSON formatting of API calls
  private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  /**
   * Construct credentials from the passed Configuration.
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on General Security Error.
   */
  public Credential createBigQueryCredential(Configuration config)
      throws GeneralSecurityException, IOException {

      return HadoopCredentialConfiguration
          .newBuilder()
          .withConfiguration(config)
          .withOverridePrefix(BIGQUERY_CONFIG_PREFIX)
          .build()
          .getCredential(BIGQUERY_OAUTH_SCOPES);
  }

  /**
   * Constructs a BigQuery from the credential constructed from the environment.
   *
   * @throws IOException on IO Error.
   * @throws GeneralSecurityException on General Security Error.
   */
  public Bigquery getBigQuery(Configuration config)
      throws GeneralSecurityException, IOException {
    log.info("Creating BigQuery from default credential.");
    Credential credential = createBigQueryCredential(config);
    // Use the credential to create an authorized BigQuery client
    return getBigQueryFromCredential(credential, BQC_ID);
  }

  /**
   * Constructs a BigQuery from a given Credential.
   */
  public Bigquery getBigQueryFromCredential(Credential credential, String appName) {
    log.info("Creating BigQuery from given credential.");
    // Use the credential to create an authorized BigQuery client
    if (credential != null) {
      return new Bigquery
          .Builder(HTTP_TRANSPORT, JSON_FACTORY, new RetryHttpInitializer(credential, appName))
          .setApplicationName(appName).build();
    }
    return new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
        .setApplicationName(appName).build();
  }
}
