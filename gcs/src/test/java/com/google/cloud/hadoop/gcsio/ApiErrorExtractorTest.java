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

package com.google.cloud.hadoop.gcsio;

import static org.mockito.Mockito.mock;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Unit-tests for ApiErrorExtractor class.
 */
@RunWith(JUnit4.class)
public class ApiErrorExtractorTest {

  /**
   * Wraps ApiErrorExtractor class so that getHttpStatusCode() can be overriden.
   *
   * Note: GoogleJsonResponseException.getStatusCode() method is marked final therefore
   * it cannot be mocked using Mockito. We use this wrapper so that we can override it in tests.
   */
  class ApiErrorExtractorWrapper
      extends ApiErrorExtractor {

    // Code to return when getHttpStatusCode() is called.
    public int code;

    /**
     * Returns this.code regardless of the given exception instance.
     */
    @Override
    int getHttpStatusCode(GoogleJsonResponseException e) {
      int ignored = super.getHttpStatusCode(e);
      return code;
    }
  }

  // Instance of ApiErrorExtractorWrapper used in tests.
  private ApiErrorExtractorWrapper errorExtractor = new ApiErrorExtractorWrapper();

  // Instance of GoogleJsonResponseException used in tests.
  private GoogleJsonResponseException e = mock(GoogleJsonResponseException.class);

  /**
   * Validates itemNotFound().
   */
  @Test
  public void testItemNotFound() {
    // Check success cases.
    errorExtractor.code = HttpStatusCodes.STATUS_CODE_NOT_FOUND;
    Assert.assertTrue(errorExtractor.itemNotFound(e));
    GoogleJsonError gje = new GoogleJsonError();
    gje.setCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND);
    Assert.assertTrue(errorExtractor.itemNotFound(gje));

    // Check failure case.
    errorExtractor.code = HttpStatusCodes.STATUS_CODE_OK;
    Assert.assertFalse(errorExtractor.itemNotFound(e));
    Assert.assertFalse(errorExtractor.itemNotFound(new IOException()));

    // If itemNotFound() is changed in future to look at inner exceptions,
    // then this test will fail indicating that we do not want to support looking at
    // inner exceptions for this purpose.
    errorExtractor.code = HttpStatusCodes.STATUS_CODE_NOT_FOUND;
    Assert.assertFalse(errorExtractor.itemNotFound(new IOException(e)));
  }

  /**
   * Validates accessDenied().
   */
  @Test
  public void testAccessDenied() {
    // Check success case.
    errorExtractor.code = HttpStatusCodes.STATUS_CODE_FORBIDDEN;
    Assert.assertTrue(errorExtractor.accessDenied(e));
    Assert.assertTrue(errorExtractor.accessDenied(new IOException(e)));

    // Check failure case.
    errorExtractor.code = HttpStatusCodes.STATUS_CODE_OK;
    Assert.assertFalse(errorExtractor.accessDenied(e));
    Assert.assertFalse(errorExtractor.accessDenied(new IOException(e)));
  }

  /**
   * Validates rangeNotSatisfiable().
   */
  @Test
  public void testRangeNotSatisfiable() {
    // Check success case.
    errorExtractor.code = ApiErrorExtractor.STATUS_CODE_RANGE_NOT_SATISFIABLE;
    Assert.assertTrue(errorExtractor.rangeNotSatisfiable(e));

    // Doesn't suffice to have getCause() be the correct exception.
    Assert.assertFalse(errorExtractor.rangeNotSatisfiable(new IOException(e)));

    // Check failure case.
    errorExtractor.code = HttpStatusCodes.STATUS_CODE_OK;
    Assert.assertFalse(errorExtractor.rangeNotSatisfiable(e));
  }
}
