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

import java.util.Optional;
import org.apache.hadoop.util.VersionInfo;

/**
 * Provides information regarding the currently loaded Hadoop version.
 */
public class HadoopVersionInfo {

  private static final HadoopVersionInfo INSTANCE = new HadoopVersionInfo();
  public static HadoopVersionInfo getInstance() {
    return INSTANCE;
  }

  private Optional<Integer> majorVersion = Optional.empty();
  private Optional<Integer> minorVersion = Optional.empty();
  private Optional<Integer> patchLevel = Optional.empty();

  /**
   * Construct a HadoopVersionInfo using the version string contained in
   * {@link VersionInfo#getVersion()}.
   */
  public HadoopVersionInfo() {
    this(VersionInfo.getVersion());
  }

  /**
   * Construct a HadoopVersionInfo using the passed version string
   * @param versionString A string of the form major.minor.patch
   */
  public HadoopVersionInfo(String versionString) {
    parseVersionString(versionString);
  }

  /**
   * ParseState is used by subclasses that are parsing version strings.
   */
  protected static class ParseState {
    public final String input;
    public int currentIndex;

    ParseState(String input) {
      this.input = input;
      this.currentIndex = 0;
    }

    public void advanceInput(int characters) {
      currentIndex += characters;
    }

    public boolean isAtEndOfInput() {
      return currentIndex >= input.length();
    }

    public char currentCharacter() {
      return input.charAt(currentIndex);
    }
  }

  // version := <majorVersion> (<separator> <minorVersion>)* (<separator> <patch>)*
  // majorVersion := <numberPiece>
  // minorVersion := <numberPiece>
  // patch := <numberPiece>
  // numberPiece = [0-9]+
  // separator := [-.]
  protected boolean parseVersionString(String version) {
    ParseState state = new ParseState(version);

    majorVersion = parseNumberPiece(state);
    if (majorVersion.isPresent()) {
      discardSeparator(state);
      minorVersion = parseNumberPiece(state);
      if (minorVersion.isPresent()) {
        discardSeparator(state);
        patchLevel = parseNumberPiece(state);
      }
    }

    return majorVersion.isPresent();
  }

  protected Optional<Integer> parseNumberPiece(ParseState state) {
    StringBuilder builder = new StringBuilder(state.input.length());
    while (!state.isAtEndOfInput() && Character.isDigit(state.currentCharacter())) {
      builder.append(state.currentCharacter());
      state.advanceInput(1);
    }

    if (builder.length() > 0) {
      try {
        return Optional.of(Integer.parseInt(builder.toString(), 10));
      } catch (NumberFormatException nfe) {
        throw new IllegalStateException(
            "Builder composed of digits could not be parsed as a number", nfe);
      }
    }

    return Optional.empty();
  }

  protected void discardSeparator(ParseState state) {
    while (!state.isAtEndOfInput()
        && (state.currentCharacter() == '.' || state.currentCharacter() == '-')) {
      state.advanceInput(1);
    }
  }

  /**
   * Get the major part of the version.
   */
  public Optional<Integer> getMajorVersion() {
    return majorVersion;
  }

  /**
   * Get the minor part of the version
   */
  public Optional<Integer> getMinorVersion() {
    return minorVersion;
  }

  /**
   * Get the patch level of the version
   */
  public Optional<Integer> getPatchLevel() {
    return patchLevel;
  }

  /**
   * Determine if this object is greater than the passed version number.
   * In order for a component to be included in the calculation it must be present in the version
   * string.
   * @param major The major number to compare against
   * @param minor The minor number to compare against
   * @return True if the version curently running is greater than
   *    the given major and minor version numbers.
   */
  public boolean isGreaterThan(int major, int minor) {
    return majorVersion.isPresent() && majorVersion.get() > major
         || (majorVersion.isPresent() && majorVersion.get() == major
             && minorVersion.isPresent() && minorVersion.get() > minor);
  }

  /**
   * Determine if this object is less than the passed version number.
   * In order for a component to be included in the calculation it must be present in the version
   * string.
   * @param major The major number to compare against
   * @param minor The minor number to compare against
   * @return True if the version curently running is less than
   *    the given major and minor version numbers.
   */
  public boolean isLessThan(int major, int minor) {
    return majorVersion.isPresent() && majorVersion.get() < major
        || (majorVersion.isPresent() && majorVersion.get() ==  major
            && minorVersion.isPresent() && minorVersion.get() < minor);
  }

  /**
   * Determine if the object is equal to the given major / minor versions
   * @param major The major version number
   * @param minor The minor version number
   * @return True if the version curently running matches both the major and minor version numbers.
   */
  public boolean isEqualTo(int major, int minor) {
    return majorVersion.isPresent() && majorVersion.get() == major
        && minorVersion.isPresent() && minorVersion.get() == minor;
  }

}
