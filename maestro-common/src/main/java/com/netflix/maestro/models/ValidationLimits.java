/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.models;

/**
 * Static holder for configurable validation limits. Validators in maestro-common read from here
 * instead of directly from {@link Constants}. At startup, the Spring layer calls {@link
 * #initialize} to override the defaults with values from application config.
 */
public final class ValidationLimits {
  private ValidationLimits() {}

  private static volatile int idLengthLimit = Constants.ID_LENGTH_LIMIT;
  private static volatile int nameLengthLimit = Constants.NAME_LENGTH_LIMIT;

  /** Returns the current ID length limit used by validators. */
  public static int getIdLengthLimit() {
    return idLengthLimit;
  }

  /** Returns the current name length limit used by validators. */
  public static int getNameLengthLimit() {
    return nameLengthLimit;
  }

  /**
   * Overrides the default limits. Called once at application startup by the Spring configuration
   * layer. Values must be positive integers.
   */
  public static void initialize(int idLimit, int nameLimit) {
    if (idLimit <= 0 || nameLimit <= 0) {
      throw new IllegalArgumentException(
          "Validation limits must be positive: idLengthLimit="
              + idLimit
              + ", nameLengthLimit="
              + nameLimit);
    }
    idLengthLimit = idLimit;
    nameLengthLimit = nameLimit;
  }
}
