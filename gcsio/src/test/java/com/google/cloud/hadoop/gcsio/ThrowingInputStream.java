/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

/**
 * Test implementation of the {@link InputStream} that throws exception in {@code #read()} and/or
 * {@code close()} methods
 */
public class ThrowingInputStream extends InputStream {

  private final Throwable readException;
  private final Throwable closeException;

  public ThrowingInputStream(Throwable readException) {
    this(readException, /* closeException= */ null);
  }

  public ThrowingInputStream(Throwable readException, Throwable closeException) {
    this.readException = readException;
    this.closeException = closeException;
  }

  @Override
  public int available() {
    return 1;
  }

  @Override
  public int read() {
    throwUnchecked(readException);
    fail("Exception should have been thrown");
    return -1;
  }

  @Override
  public void close() throws IOException {
    if (closeException != null) {
      throwUnchecked(closeException);
    }
    super.close();
  }

  public static void throwUnchecked(Throwable e) {
    throwAny(e);
    fail(String.format("Exception '%s' should have been thrown", e));
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwAny(Throwable e) throws E {
    throw (E) e;
  }
}
