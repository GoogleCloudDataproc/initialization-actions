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

package com.google.cloud.hadoop.gcsio;

/**
 * MethodOutcome encapsulates the possible outcomes of calling a method which returns a boolean
 * value; either "return true", "return false", or "throw exception"; the latter case will also
 * have an associated expected Exception class.
 */
public class MethodOutcome {
  /**
   * The possible high-level categories of outcomes which are mutually exclusive; this way, there
   * is no need to worry about whether some boolean is necessarily "true" or "false" in the case
   * of an exception being thrown, since there will be no return value in such a case.
   */
  public static enum Type {
    RETURNS_TRUE,
    RETURNS_FALSE,
    THROWS_EXCEPTION
  }
  private Type expectedOutcome;
  private Class<? extends Exception> expectedExceptionClass;

  public MethodOutcome(Type expectedOutcome) {
    this.expectedOutcome = expectedOutcome;
    this.expectedExceptionClass = null;
  }

  public MethodOutcome(
      Type expectedOutcome, Class<? extends Exception> expectedExceptionClass) {
    this.expectedOutcome = expectedOutcome;
    this.expectedExceptionClass = expectedExceptionClass;
  }

  public String toString() {
    switch (expectedOutcome) {
      case RETURNS_TRUE:
        return "Returns true";
      case RETURNS_FALSE:
        return "Returns false";
      case THROWS_EXCEPTION:
        return "Throws " + expectedExceptionClass;
      default:
        return "Unknown MethodOutcome.Type: " + expectedOutcome;
    }
  }

  /**
   * Accessor for expected Type enum.
   */
  public Type getType() {
    return expectedOutcome;
  }

  /**
   * Accessor for expected Exception class.
   */
  public Class<? extends Exception> getExceptionClass() {
    return expectedExceptionClass;
  }
}
