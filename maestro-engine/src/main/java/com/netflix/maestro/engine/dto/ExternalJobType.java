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
package com.netflix.maestro.engine.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;

/** Enum for specifying external job type. */
public enum ExternalJobType {
  /** Titus Job Type. */
  TITUS;

  /**
   * JSONCreator for OutputParameter.
   *
   * @param externalJobType external job type in string format
   * @return enum type
   */
  @JsonCreator
  public static ExternalJobType forValues(String externalJobType) {
    return ExternalJobType.valueOf(externalJobType.toUpperCase(Locale.US));
  }
}
