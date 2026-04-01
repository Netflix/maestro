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
package com.netflix.maestro.utils;

import com.netflix.maestro.models.Constants;

/** Interface exposing configurable validation limits used by constraint validators. */
public interface ValidationLimits {

  /**
   * Default instance backed by compile-time {@link Constants} values. Used by validators when no
   * configured {@link ValidationLimits} bean is injected (e.g. in unit tests without Spring).
   * Keeps each limit independently defaulted so id and name limits remain decoupled.
   */
  ValidationLimits DEFAULTS =
      new ValidationLimits() {
        @Override
        public int getIdLengthLimit() {
          return Constants.ID_LENGTH_LIMIT;
        }

        @Override
        public int getNameLengthLimit() {
          return Constants.NAME_LENGTH_LIMIT;
        }
      };

  /** Returns the maximum allowed length for Maestro IDs. */
  int getIdLengthLimit();

  /** Returns the maximum allowed length for Maestro names. */
  int getNameLengthLimit();
}
