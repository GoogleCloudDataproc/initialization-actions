/**
 * Copyright 2013 Google Inc. All Rights Reserved.
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

import com.google.common.base.Throwables;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;

/**
 * Enables synchronized and parameterized logging over Apache logging helpers.
 */
public class LogUtil {

  /**
   * Enumeration of possible log levels which is independent of logger implementation-specific
   * definitions of log levels. Some implementations may require imperfect matching of one
   * of our level definitions.
   */
  public static enum Level {
    ALL,
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL,
    OFF
  }

  // Apache logger.
  private Log log;

  /**
   * Constructs a LogUtil instance.
   *
   * @param clazz The class that uses this logger.
   */
  public LogUtil(Class<? extends Object> clazz) {
    log = LogFactory.getLog(clazz);
  }

  /**
   * For known underlying logger types, tries to set the logging level. We will provide a
   * possible-imperfect mapping from our generalized levels to the implementation-specific
   * level.
   * Currently only supports:
   * 1. Log4JLogger
   *
   * @throws RuntimeException if runtime logger type is not a supported type.
   */
  public synchronized void setLevel(Level level) {
    if (log instanceof Log4JLogger) {
      Log4JLogger wrapper = (Log4JLogger) log;
      org.apache.log4j.Logger typedLogger = wrapper.getLogger();
      switch (level) {
        case TRACE:
          typedLogger.setLevel(org.apache.log4j.Level.TRACE);
          break;
        case DEBUG:
          typedLogger.setLevel(org.apache.log4j.Level.DEBUG);
          break;
        case INFO:
          typedLogger.setLevel(org.apache.log4j.Level.INFO);
          break;
        case WARN:
          typedLogger.setLevel(org.apache.log4j.Level.WARN);
          break;
        case ERROR:
          typedLogger.setLevel(org.apache.log4j.Level.ERROR);
          break;
        case FATAL:
          typedLogger.setLevel(org.apache.log4j.Level.FATAL);
          break;
        case OFF:
          typedLogger.setLevel(org.apache.log4j.Level.OFF);
          break;
        default:
          throw new RuntimeException("Unknown level: " + level);
      }
    } else {
      throw new RuntimeException("Unrecognized logger type: " + log.getClass());
    }
  }

  /**
   * Logs a debug message.
   *
   * @param message Format of the message to log.
   * @param args Optional message parameters.
   */
  public synchronized void debug(String message, Object... args) {
    if (log.isDebugEnabled()) {
      log.debug(formatMessage(message, args));
    }
  }

  /**
   * Logs a debug message.
   *
   * @param message Message to log.
   * @param t Throwable to log.
   */
  public synchronized void debug(String message, Throwable t) {
    if (log.isDebugEnabled()) {
      debug("%s\n%s\n%s", message, t.toString(), Throwables.getStackTraceAsString(t));
    }
  }

  /**
   * Logs a debug message.
   *
   * @param t Throwable to log.
   */
  public synchronized void debug(Throwable t) {
    debug("", t);
  }

  /**
   * Logs an informational message.
   *
   * @param message Format of the message to log.
   * @param args Optional message parameters.
   */
  public synchronized void info(String message, Object... args) {
    if (log.isInfoEnabled()) {
      log.info(formatMessage(message, args));
    }
  }

  /**
   * Logs an informational message.
   *
   * @param message Message to log.
   * @param t Throwable to log.
   */
  public synchronized void info(String message, Throwable t) {
    if (log.isInfoEnabled()) {
      info("%s\n%s\n%s", message, t.toString(), Throwables.getStackTraceAsString(t));
    }
  }

  /**
   * Logs an informational message.
   *
   * @param t Throwable to log.
   */
  public synchronized void info(Throwable t) {
    info("", t);
  }

  /**
   * Logs a warning message.
   *
   * @param message Format of the message to log.
   * @param args Optional message parameters.
   */
  public synchronized void warn(String message, Object... args) {
    if (log.isWarnEnabled()) {
      log.warn(formatMessage(message, args));
    }
  }

  /**
   * Logs a warning message.
   *
   * @param message Message to log.
   * @param t Throwable to log.
   */
  public synchronized void warn(String message, Throwable t) {
    if (log.isWarnEnabled()) {
      warn("%s\n%s\n%s", message, t.toString(), Throwables.getStackTraceAsString(t));
    }
  }

  /**
   * Logs a warning message.
   *
   * @param t Throwable to log.
   */
  public synchronized void warn(Throwable t) {
    warn("", t);
  }

  /**
   * Logs an error message.
   *
   * @param message Format of the message to log.
   * @param args Optional message parameters.
   */
  public synchronized void error(String message, Object... args) {
    if (log.isErrorEnabled()) {
      log.error(formatMessage(message, args));
    }
  }

  /**
   * Logs an error message.
   *
   * @param message Message to log.
   * @param t Throwable to log.
   */
  public synchronized void error(String message, Throwable t) {
    if (log.isErrorEnabled()) {
      error("%s\n%s\n%s", message, t.toString(), Throwables.getStackTraceAsString(t));
    }
  }

  /**
   * Logs an error message.
   *
   * @param t Throwable to log.
   */
  public synchronized void error(Throwable t) {
    error("", t);
  }

  /**
   * Formats a message.
   *
   * @param message Format of the message.
   * @param args Optional message parameters.
   * @return Formatted message or null if this message should not be logged.
   */
  private synchronized String formatMessage(String message, Object... args) {
    if (args.length > 0) {
      message = String.format(message, args);
    }

    return message;
  }

  /**
   * Is debug logging currently enabled?
   *
   * @return true if debug is enabled in the underlying logger.
   */
  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }
}
