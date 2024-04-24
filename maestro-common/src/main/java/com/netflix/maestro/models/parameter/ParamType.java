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
package com.netflix.maestro.models.parameter;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;

/** Supported parameter type. */
public enum ParamType {
  /** Explicit type info for result returning STRING. */
  STRING,
  /** Explicit type info for result returning LONG. */
  LONG,
  /** Explicit type info for result returning DOUBLE. */
  DOUBLE,
  /** Explicit type info for result returning BOOLEAN. */
  BOOLEAN,
  /** Explicit type info for result returning MAP<String, String>. */
  STRING_MAP,
  /** Explicit type info for result returning STRING_ARRAY. */
  STRING_ARRAY,
  /** Explicit type info for result returning LONG_ARRAY. */
  LONG_ARRAY,
  /** Explicit type info for result returning DOUBLE_ARRAY. */
  DOUBLE_ARRAY,
  /** Explicit type info for result returning BOOLEAN_ARRAY. */
  BOOLEAN_ARRAY,
  /** Explicit type info for result returning MAP<String, Object>. */
  MAP,
  /** Explicit type info for result returning SIGNAL. */
  SIGNAL;

  /** JSON creator. */
  @JsonCreator
  public static ParamType create(String type) {
    return ParamType.valueOf(type.toUpperCase(Locale.US));
  }
}
