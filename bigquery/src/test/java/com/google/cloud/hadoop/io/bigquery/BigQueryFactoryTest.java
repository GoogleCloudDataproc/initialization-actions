/*
 * Copyright 2017 Google LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.io.bigquery;

import static com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration.BQ_ROOT_URL;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.Bigquery;
import com.google.cloud.hadoop.util.testing.CredentialConfigurationUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for BigQueryFactory.
 *
 * TODO(user): implement manual integration tests to check for successful connections.
 */
@RunWith(JUnit4.class)
public class BigQueryFactoryTest {

  @Test
  public void getBigQuery_localDev() throws Exception {
    BigQueryFactory factory = new BigQueryFactory();
    Configuration configuration = CredentialConfigurationUtil.getTestConfiguration();

    Bigquery bigquery = factory.getBigQuery(configuration);

    assertThat(bigquery).isNotNull();
    assertThat(bigquery.getRootUrl()).isEqualTo("https://bigquery.googleapis.com/");
  }

  @Test
  public void getBigQuery_customEndpoint() throws Exception {
    BigQueryFactory factory = new BigQueryFactory();
    Configuration configuration = CredentialConfigurationUtil.getTestConfiguration();
    configuration.set(BQ_ROOT_URL.getKey(), "https://unit-test-bigquery.googleapis.com/");

    Bigquery bigquery = factory.getBigQuery(configuration);

    assertThat(bigquery).isNotNull();
    assertThat(bigquery.getRootUrl()).isEqualTo("https://unit-test-bigquery.googleapis.com/");
  }

  @Test
  public void testVersionString() {
    assertThat(BigQueryFactory.VERSION).isNotNull();
    assertThat(BigQueryFactory.UNKNOWN_VERSION.equals(BigQueryFactory.VERSION)).isFalse();
  }
}
