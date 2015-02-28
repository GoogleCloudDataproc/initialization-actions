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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HadoopVersionInfoTest {

  public static void assertVersionComponents(
      HadoopVersionInfo version, int major, int minor, int patchLevel) {

    Assert.assertTrue(
        "Expected major version to be present.",
        version.getMajorVersion().isPresent());
    Assert.assertEquals(major, version.getMajorVersion().get().intValue());

    Assert.assertTrue(
        "Expected minor version to be present.",
        version.getMinorVersion().isPresent());
    Assert.assertEquals(minor, version.getMinorVersion().get().intValue());

    Assert.assertTrue(
        "Expected patch level to be present.",
        version.getPatchLevel().isPresent());
    Assert.assertEquals(patchLevel, version.getPatchLevel().get().intValue());
  }

  @Test
  public void normalVersionStringParsed() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("2.1.0");
    assertVersionComponents(versionInfo, 2, 1, 0);

    versionInfo = new HadoopVersionInfo("1.2.1-dev");
    assertVersionComponents(versionInfo, 1, 2, 1);

    versionInfo = new HadoopVersionInfo("0.20.205");
    assertVersionComponents(versionInfo, 0, 20, 205);

    versionInfo = new HadoopVersionInfo("0.20.205-dev");
    assertVersionComponents(versionInfo, 0, 20, 205);
  }

  @Test
  public void defaultVersionParsed() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo();
    Assert.assertTrue(
        "Expected major version to be present.",
        versionInfo.getMajorVersion().isPresent());

    Assert.assertTrue(
        "Expected minor version to be present.",
        versionInfo.getMinorVersion().isPresent());

    Assert.assertTrue(
        "Expected patch level to be present.",
        versionInfo.getPatchLevel().isPresent());
  }

  @Test
  public void majorOnlyVersionStringParsed() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("2");

    Assert.assertTrue(
        "Expected major version to be present.",
        versionInfo.getMajorVersion().isPresent());
    Assert.assertEquals(2, versionInfo.getMajorVersion().get().intValue());

    Assert.assertFalse(
        "Did not expect minor version to be present.",
        versionInfo.getMinorVersion().isPresent());

    Assert.assertFalse(
        "Did not expect patch level to be present.",
        versionInfo.getPatchLevel().isPresent());
  }

  public void assertAllInfoAbsent(HadoopVersionInfo version) {
    Assert.assertFalse(
        "Did not expect major version to be present.",
        version.getMajorVersion().isPresent());

    Assert.assertFalse(
        "Did not expect minor version to be present.",
        version.getMinorVersion().isPresent());

    Assert.assertFalse(
        "Did not expect patch level to be present.",
        version.getPatchLevel().isPresent());
  }

  @Test
  public void junkStringResultsInAbsentVersions() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("ThisIsAJunkString");
    assertAllInfoAbsent(versionInfo);

    versionInfo = new HadoopVersionInfo(".1.2.3");
    assertAllInfoAbsent(versionInfo);
  }

  @Test
  public void testIsGreaterThan() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("2.3.0");
    Assert.assertTrue(
        "Expected 2.3.0 to be greater than 2.1.0",
        versionInfo.isGreaterThan(2, 1));

    Assert.assertFalse(
        "Did NOT expect 2.3 to be greater than 3.1",
        versionInfo.isGreaterThan(3, 1));

    versionInfo = new HadoopVersionInfo("3.2.0");
    Assert.assertTrue(
        "Expected 3.2.0 to be greater than 2.1.0",
        versionInfo.isGreaterThan(2, 1));

    versionInfo = new HadoopVersionInfo("1.2.1");
    Assert.assertFalse(
        "Expected 1.2.1 to be NOT greater than 2.3.0",
        versionInfo.isGreaterThan(2, 3));

    versionInfo = new HadoopVersionInfo("Junk");
    Assert.assertFalse(
        "Nothing should be greater than an unknown version",
        versionInfo.isGreaterThan(0, 0));
  }

  @Test
  public void testIsLessThan() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("2.3.0");
    Assert.assertTrue(
        "Expected 2.3.0 to be less than 2.4.0",
        versionInfo.isLessThan(2, 4));

    Assert.assertFalse(
        "Did NOT expect 2.3 to be less than 1.2",
        versionInfo.isLessThan(1, 2));

    versionInfo = new HadoopVersionInfo("1.2.0");
    Assert.assertTrue(
        "Expected 1.2.0 to be less than 2.1.0",
        versionInfo.isLessThan(2, 1));

    versionInfo = new HadoopVersionInfo("2.2.0");
    Assert.assertFalse(
        "Expected 2.2.0 to be NOT less than 1.2.0",
        versionInfo.isLessThan(1, 2));

    versionInfo = new HadoopVersionInfo("Junk");
    Assert.assertFalse(
        "Nothing should be less than an unknown version",
        versionInfo.isLessThan(Integer.MAX_VALUE, Integer.MAX_VALUE));
  }

  @Test
  public void testIsEqualTo() {
    HadoopVersionInfo versionInfo = new HadoopVersionInfo("2.3.0");
    Assert.assertTrue("Expected 2.3.0 to equal 2.3", versionInfo.isEqualTo(2, 3));
    Assert.assertFalse("Did not expect 2.3.0 to equal 2.2", versionInfo.isEqualTo(2, 2));
    Assert.assertFalse("Did not expect 2.3.0 to equal 1.3", versionInfo.isEqualTo(1, 3));
  }
}
