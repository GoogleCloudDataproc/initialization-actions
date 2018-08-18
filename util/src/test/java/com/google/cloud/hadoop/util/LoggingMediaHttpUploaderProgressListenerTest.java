/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import com.google.api.client.googleapis.media.MediaHttpUploader.UploadState;
import com.google.common.flogger.LoggerConfig;
import java.io.ByteArrayOutputStream;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link LoggingMediaHttpUploaderProgressListener}. */
@RunWith(JUnit4.class)
public class LoggingMediaHttpUploaderProgressListenerTest {

  private static final Formatter LOG_FORMATTER =
      new Formatter() {
        @Override
        public String format(LogRecord record) {
          return record.getLevel() + ": " + record.getMessage();
        }
      };

  private LoggingMediaHttpUploaderProgressListener listener;

  private ByteArrayOutputStream logCapturingStream;
  private StreamHandler customLogHandler;

  @Before
  public void setUp() {
    listener = new LoggingMediaHttpUploaderProgressListener("NAME", 60000L);

    LoggerConfig config = LoggerConfig.getConfig(LoggingMediaHttpUploaderProgressListener.class);
    config.setLevel(Level.FINE);

    logCapturingStream = new ByteArrayOutputStream();
    customLogHandler = new StreamHandler(logCapturingStream, LOG_FORMATTER);
    customLogHandler.setLevel(Level.FINE);
    config.addHandler(customLogHandler);
  }

  @After
  public void verifyAndRemoveAssertingHandler() {
    LoggerConfig.getConfig(LoggingMediaHttpUploaderProgressListener.class)
        .removeHandler(customLogHandler);
  }

  @Test
  public void testLoggingInitiation() {
    listener.progressChanged(UploadState.INITIATION_STARTED, 0L, 0L);
    assertThat(getTestCapturedLog()).isEqualTo("FINE: Uploading: NAME");
  }

  @Test
  public void testLoggingProgressAfterSixtySeconds() {
    listener.progressChanged(UploadState.MEDIA_IN_PROGRESS, 10485760L, 60001L);
    assertThat(getTestCapturedLog())
        .isEqualTo(
            "FINE: Uploading:"
                + " NAME Average Rate: 0.167 MiB/s, Current Rate: 0.167 MiB/s, Total: 10.000 MiB");

    listener.progressChanged(UploadState.MEDIA_IN_PROGRESS, 104857600L, 120002L);
    assertThat(getTestCapturedLog())
        .isEqualTo(
            "FINE: Uploading:"
                + " NAME Average Rate: 0.833 MiB/s, Current Rate: 1.500 MiB/s, Total: 100.000 MiB");
  }

  @Test
  public void testSkippingLoggingAnInProgressUpdate() {
    listener.progressChanged(UploadState.MEDIA_IN_PROGRESS, 104857600L, 60000L);
    assertThat(getTestCapturedLog()).isEmpty();
  }

  @Test
  public void testLoggingCompletion() {
    listener.progressChanged(UploadState.MEDIA_COMPLETE, 104857600L, 60000L);
    assertThat(getTestCapturedLog()).isEqualTo("FINE: Finished Uploading: NAME");
  }

  @Test
  public void testOtherUpdatesIgnored() {
    listener.progressChanged(UploadState.NOT_STARTED, 0L, 60001L);
    listener.progressChanged(UploadState.INITIATION_COMPLETE, 0L, 60001L);
    assertThat(getTestCapturedLog()).isEmpty();
  }

  private String getTestCapturedLog() {
    customLogHandler.flush();
    try {
      return logCapturingStream.toString();
    } finally {
      logCapturingStream.reset();
    }
  }
}
