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

/** InternalParamMode set by system, not exposed after merge. */
public enum InternalParamMode {
  /** The parameter value is required. */
  REQUIRED,
  /** The parameter value is optional. */
  OPTIONAL,
  /** The parameter value is provided by the system. */
  PROVIDED,
  /** The parameter is reserved by the system, cannot be passed by user. */
  RESERVED;

  /** ParamMode creator. */
  @JsonCreator
  public static InternalParamMode create(String mode) {
    return InternalParamMode.valueOf(mode.toUpperCase(Locale.US));
  }
}
