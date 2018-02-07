/**
 * Copyright 2014 Google Inc. All Rights Reserved.
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


package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.expectThrows;

import com.google.api.client.util.BackOff;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link RetryBoundedBackOff}. */
@RunWith(JUnit4.class)
public class RetryBoundedBackOffTest {
  @Test
  public void testValidCallHasNoRetries() throws Exception {
    IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class, () -> new RetryBoundedBackOff(-7, new BackOffTester()));
    assertThat(thrown)
        .hasMessageThat()
        .contains("Maximum number of retries must not be less than 0.");
  }

  @Test
  public void stopsAfterAttempts() throws Exception {
    BackOff backoff = new RetryBoundedBackOff(5, new BackOffTester());
    for (int i = 0; i < 5; i++) {
      assertThat(i + 1).isEqualTo(backoff.nextBackOffMillis());
    }
    assertThat(backoff.nextBackOffMillis()).isEqualTo(BackOff.STOP);
    assertThat(backoff.nextBackOffMillis()).isEqualTo(BackOff.STOP);
  }

  @Test
  public void resetsCorrectly() throws Exception {
    BackOff backoff = new RetryBoundedBackOff(5, new BackOffTester());
    for (int i = 0; i < 5; i++) {
      assertThat(i + 1).isEqualTo(backoff.nextBackOffMillis());
    }
    assertThat(backoff.nextBackOffMillis()).isEqualTo(BackOff.STOP);
    assertThat(backoff.nextBackOffMillis()).isEqualTo(BackOff.STOP);
    backoff.reset();
    for (int i = 0; i < 3; i++) {
      assertThat(i + 1).isEqualTo(backoff.nextBackOffMillis());
    }
    backoff.reset();
    for (int i = 0; i < 5; i++) {
      assertThat(i + 1).isEqualTo(backoff.nextBackOffMillis());
    }
    assertThat(backoff.nextBackOffMillis()).isEqualTo(BackOff.STOP);
    assertThat(backoff.nextBackOffMillis()).isEqualTo(BackOff.STOP);
  }

  /** A simple {@link BackOff} to help with testing. */
  public class BackOffTester implements BackOff {
    public int counter = 0;

    @Override
    public void reset() {
      counter = 0;
    }

    @Override
    public long nextBackOffMillis() throws IOException {
      return ++counter;
    }
  }
}
