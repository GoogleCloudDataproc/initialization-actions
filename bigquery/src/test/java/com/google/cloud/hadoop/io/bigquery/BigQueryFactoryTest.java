package com.google.cloud.hadoop.io.bigquery;

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.hadoop.testing.CredentialConfigurationUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Unit tests for BigQueryFactory.
 *
 * TODO(user): implement manual integration tests to check for successful connections.
 */
@RunWith(JUnit4.class)
public class BigQueryFactoryTest {
  /**
   * Test for getBigQuery method. This should return a BigQuery set up for local development.
   */
  @Test
  public void testGetBigQuery()
      throws GeneralSecurityException, IOException {
    BigQueryFactory factory = new BigQueryFactory();
    Configuration configuration = CredentialConfigurationUtil.getTestConfiguration();
    Bigquery bigquery = factory.getBigQuery(configuration);
    Assert.assertTrue(bigquery != null);
    assertEquals("https://www.googleapis.com/", bigquery.getRootUrl());
  }

  @Test
  public void testVersionString() {
    Assert.assertNotNull(BigQueryFactory.VERSION);
    Assert.assertFalse(
        BigQueryFactory.UNKNOWN_VERSION.equals(BigQueryFactory.VERSION));
  }
}
