/*
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.hadoop.util.VersionInfo;

/** Provides information regarding the currently loaded Hadoop version. */
public class HadoopVersionInfo {

  private static final HadoopVersionInfo INSTANCE = new HadoopVersionInfo(VersionInfo.getVersion());

  /**
   * Returns a HadoopVersionInfo that uses a version string contained in {@link
   * VersionInfo#getVersion()}.
   */
  public static HadoopVersionInfo getInstance() {
    return INSTANCE;
  }

  private final Integer majorVersion;
  private final Integer minorVersion;
  private final Integer subminorVersion;

  /**
   * Construct a HadoopVersionInfo using the passed version string
   *
   * @param versionString A string of the form major.minor.subminor
   */
  @VisibleForTesting
  HadoopVersionInfo(String versionString) {
    String[] versionPieces = versionString.split("[^0-9]+", 4);
    majorVersion = parseNumberPiece(versionPieces, 0);
    minorVersion = majorVersion == null ? null : parseNumberPiece(versionPieces, 1);
    subminorVersion = minorVersion == null ? null : parseNumberPiece(versionPieces, 2);
  }

  private Integer parseNumberPiece(String[] pieces, int index) {
    if (index >= pieces.length || pieces[index].isEmpty()) {
      return null;
    }
    try {
      return Integer.parseInt(pieces[index], 10);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Failed to parse version piece as a number: " + pieces[index], e);
    }
  }

  /** Get the major part of the version. */
  public Optional<Integer> getMajorVersion() {
    return Optional.fromNullable(majorVersion);
  }

  /** Get the minor part of the version */
  public Optional<Integer> getMinorVersion() {
    return Optional.fromNullable(minorVersion);
  }

  /** Get the subminor part of the version */
  public Optional<Integer> getSubminorVersion() {
    return Optional.fromNullable(subminorVersion);
  }

  /**
   * Determine if this object is greater than the passed version number. In order for a component to
   * be included in the calculation it must be present in the version string.
   *
   * @param major The major number to compare against
   * @param minor The minor number to compare against
   * @return True if the version currently running is greater than the given major and minor version
   *     numbers.
   */
  public boolean isGreaterThan(int major, int minor) {
    return (majorVersion != null && majorVersion > major)
        || (majorVersion != null
            && majorVersion == major
            && minorVersion != null
            && minorVersion > minor);
  }

  /**
   * Determine if this object is less than the passed version number. In order for a component to be
   * included in the calculation it must be present in the version string.
   *
   * @param major The major number to compare against
   * @param minor The minor number to compare against
   * @return True if the version currently running is less than the given major and minor version
   *     numbers.
   */
  public boolean isLessThan(int major, int minor) {
    return (majorVersion != null && majorVersion < major)
        || (majorVersion != null
            && majorVersion == major
            && minorVersion != null
            && minorVersion < minor);
  }

  /**
   * Determine if the object is equal to the given major / minor versions
   *
   * @param major The major version number
   * @param minor The minor version number
   * @return True if the version currently running matches both the major and minor version numbers.
   */
  public boolean isEqualTo(int major, int minor) {
    return majorVersion != null
        && majorVersion == major
        && minorVersion != null
        && minorVersion == minor;
  }
}
