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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Unit tests for LogUtil class.
 */
@RunWith(JUnit4.class)
public class LogUtilTest {

  // Logger test instance.
  private static LogUtil log;

  // Log output goes to this writer allowing us to inspect what was logged.
  private static StringWriter sw;

  /**
   * Performs initialization once before tests are run.
   */
  @BeforeClass
  public static void beforeAllTests() {
    // Configure Log4j to send all log output to a StringWriter
    // so that we can inspect logged content.
    System.setProperty(
        "org.apache.commons.logging.Log",
        "org.apache.commons.logging.impl.Log4JLogger");
    log = new LogUtil(LogUtilTest.class);
    sw = new StringWriter();
    WriterAppender logAppender = new WriterAppender(new SimpleLayout(), sw);
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.addAppender(logAppender);
    rootLogger.setLevel(Level.DEBUG);
  }

  /**
   * Verifies that we can log messages with no parameters.
   */
  @Test
  public void messagesWithoutArgs() {
    String message = "Hello Log!";

    log.debug(message);
    verifyLoggedMessage("DEBUG", message);

    log.info(message);
    verifyLoggedMessage("INFO", message);

    log.warn(message);
    verifyLoggedMessage("WARN", message);

    log.error(message);
    verifyLoggedMessage("ERROR", message);
  }

  /**
   * Verifies that we can log messages with parameters.
   */
  @Test
  public void messagesWithArgs() {
    String message = "Hello Log!: %d";

    log.debug(message, 0);
    verifyLoggedMessage("DEBUG", message, 0);

    log.info(message, 1);
    verifyLoggedMessage("INFO", message, 1);

    log.warn(message, 2);
    verifyLoggedMessage("WARN", message, 2);

    log.error(message, 3);
    verifyLoggedMessage("ERROR", message, 3);
  }

  /**
   * Verifies that we can log messages with exception parameters.
   */
  @Test
  public void messagesWithException() {
    String message = "Hello Exception!";

    // Throw and catch an exception so that we have an instance with
    // stack trace and other info.
    Exception e;
    try {
      throw new IOException(message);
    } catch (IOException ioe) {
      e = ioe;
    }

    // Log exception and message.
    log.debug(message, e);
    verifyLoggedMessage("DEBUG", message, e);

    // Log exception.
    log.debug(e);
    verifyLoggedMessage("DEBUG", "", e);

    // Log exception and message.
    log.error(message, e);
    verifyLoggedMessage("ERROR", message, e);

    // Log exception.
    log.error(e);
    verifyLoggedMessage("ERROR", "", e);
  }

  // Maintains position of at which the last logged message ended
  // in the string buffer of the log appender.
  private static int logIndex = 0;

  /**
   * Verifies that the logged message is as expected.
   *
   * @param category Category of the message (INFO, WARN or ERROR).
   * @param message Message format string.
   * @param args Optional format parameters.
   */
  private static void verifyLoggedMessage(String category, String message, Exception e) {
    String expectedMessagePrefix =  category + " - " + message + "\n";
    expectedMessagePrefix +=  e.toString() + "\n";
    String actualMessage = sw.getBuffer().substring(logIndex);
    logIndex += actualMessage.length();
    Assert.assertEquals(
        expectedMessagePrefix,
        actualMessage.substring(0, expectedMessagePrefix.length()));
  }

  /**
   * Verifies that the logged message is as expected.
   *
   * @param category Category of the message (INFO, WARN or ERROR).
   * @param message Message format string.
   * @param args Optional format parameters.
   */
  private static void verifyLoggedMessage(
      String category, String message, Object... args) {
    if (args.length > 0) {
      message = String.format(message, args);
    }
    String expectedMessage = category + " - " + message + "\n";
    String actualMessage = sw.getBuffer().substring(logIndex);
    logIndex += actualMessage.length();
    Assert.assertEquals(expectedMessage, actualMessage);
  }
}
