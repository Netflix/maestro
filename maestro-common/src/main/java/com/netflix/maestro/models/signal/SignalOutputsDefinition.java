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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.utils.Checks;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * Data model for step's signal output definitions.
 *
 * @param definitions signal output definitions.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record SignalOutputsDefinition(@JsonValue List<SignalOutputDefinition> definitions) {
  /** Constructor. */
  @JsonCreator
  public SignalOutputsDefinition {}

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonPropertyOrder(
      value = {"name", "params", "payload"},
      alphabetic = true)
  @Data
  public static class SignalOutputDefinition {
    /** Signal name is a parsable string supporting param string interpolation. */
    private String name;

    /** Only string or long type param supported. It should not contain a param called name. */
    @Nullable private Map<String, ParamDefinition> params;

    /**
     * Additional payload kept in the signal instance, but they are not indexed or can be used for
     * signal param matching. It should not have the same key as above params.
     */
    @Nullable private Map<String, ParamDefinition> payload;

    /** set params. */
    public void setParams(Map<String, ParamDefinition> input) {
      Checks.checkTrue(
          input.values().stream()
              .allMatch(p -> p.getType() == ParamType.LONG || p.getType() == ParamType.STRING),
          "SignalOutputDefinition params [%s] only support param typ LONG or STRING",
          input);
      this.params = ParamDefinition.preprocessDefinitionParams(input);
    }

    /** set payload. */
    public void setPayload(Map<String, ParamDefinition> input) {
      this.payload = ParamDefinition.preprocessDefinitionParams(input);
    }
  }
}
