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

package com.google.cloud.hadoop.util;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopVersionInfoTest {

  @Test
  public void normalVersionString_isParsed() {
    assertVersion(new HadoopVersionInfo("2.1.0"), 2, 1, 0);
    assertVersion(new HadoopVersionInfo("1.2.1-dev"), 1, 2, 1);
    assertVersion(new HadoopVersionInfo("0.20.205"), 0, 20, 205);
    assertVersion(new HadoopVersionInfo("0.20.205-dev"), 0, 20, 205);
  }

  private static void assertVersion(HadoopVersionInfo version, int major, int minor, int subminor) {
    assertThat(version.getMajorVersion()).hasValue(major);
    assertThat(version.getMinorVersion()).hasValue(minor);
    assertThat(version.getSubminorVersion()).hasValue(subminor);
  }

  @Test
  public void defaultVersion_isParsed() {
    HadoopVersionInfo version = HadoopVersionInfo.getInstance();

    assertThat(version.getMajorVersion()).isPresent();
    assertThat(version.getMinorVersion()).isPresent();
    assertThat(version.getSubminorVersion()).isPresent();
  }

  @Test
  public void majorOnlyVersion_isParsed() {
    HadoopVersionInfo version = new HadoopVersionInfo("2");

    assertThat(version.getMajorVersion()).hasValue(2);
    assertThat(version.getMinorVersion()).isAbsent();
    assertThat(version.getSubminorVersion()).isAbsent();
  }

  @Test
  public void emptyVersion_isParsed() {
    assertAllInfoAbsent(new HadoopVersionInfo(""));
    assertAllInfoAbsent(new HadoopVersionInfo("ThisIsAJunkString"));
    assertAllInfoAbsent(new HadoopVersionInfo(".1.2.3"));
  }

  private static void assertAllInfoAbsent(HadoopVersionInfo version) {
    assertThat(version.getMajorVersion()).isAbsent();
    assertThat(version.getMinorVersion()).isAbsent();
    assertThat(version.getSubminorVersion()).isAbsent();
  }

  @Test
  public void testIsGreaterThan() {
    assertWithMessage("Expected 2.3.0 to be greater than 2.1")
        .that(new HadoopVersionInfo("2.3.0").isGreaterThan(2, 1))
        .isTrue();

    assertWithMessage("Did NOT expect 2.3.0 to be greater than 3.1")
        .that(new HadoopVersionInfo("2.3.0").isGreaterThan(3, 1))
        .isFalse();

    assertWithMessage("Expected 3.2.0 to be greater than 2.1")
        .that(new HadoopVersionInfo("3.2.0").isGreaterThan(2, 1))
        .isTrue();

    assertWithMessage("Expected 1.2.1 to be NOT greater than 2.3")
        .that(new HadoopVersionInfo("1.2.1").isGreaterThan(2, 3))
        .isFalse();
  }

  @Test
  public void testIsLessThan() {
    assertWithMessage("Expected 2.3.0 to be less than 2.4")
        .that(new HadoopVersionInfo("2.3.0").isLessThan(2, 4))
        .isTrue();

    assertWithMessage("Did NOT expect 2.3.0 to be less than 1.2")
        .that(new HadoopVersionInfo("2.3.0").isLessThan(1, 2))
        .isFalse();

    assertWithMessage("Expected 1.2.0 to be less than 2.1")
        .that(new HadoopVersionInfo("1.2.0").isLessThan(2, 1))
        .isTrue();

    assertWithMessage("Expected 2.2.0 to be NOT less than 1.2")
        .that(new HadoopVersionInfo("2.2.0").isLessThan(1, 2))
        .isFalse();
  }

  @Test
  public void testIsEqualTo() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("2.3.0");
    assertWithMessage("Expected 2.3.0 to equal 2.3").that(versionInfo.isEqualTo(2, 3)).isTrue();
    assertWithMessage("Did not expect 2.3.0 to equal 2.2")
        .that(versionInfo.isEqualTo(2, 2))
        .isFalse();
    assertWithMessage("Did not expect 2.3.0 to equal 1.3")
        .that(versionInfo.isEqualTo(1, 3))
        .isFalse();
  }
}
