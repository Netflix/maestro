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
package com.netflix.maestro.models.signal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * Data model for step's signal dependency definitions.
 *
 * @param definitions signal dependency definitions.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record SignalDependenciesDefinition(
    @JsonValue List<SignalDependencyDefinition> definitions) {
  /** Constructor. */
  @JsonCreator
  public SignalDependenciesDefinition {}

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonPropertyOrder(
      value = {"name", "match_params"},
      alphabetic = true)
  @Data
  public static class SignalDependencyDefinition {
    /** Signal name is a parsable string supporting param string interpolation. */
    private String name;

    @Nullable private Map<String, SignalMatchParamDef> matchParams;
  }
}
