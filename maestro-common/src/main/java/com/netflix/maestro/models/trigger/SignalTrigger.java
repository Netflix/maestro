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
package com.netflix.maestro.models.trigger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.signal.SignalMatchParam;
import java.util.Map;
import lombok.Data;
import lombok.ToString;

/** Data model for signal trigger defined in the workflow definition. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {"definitions", "params", "condition", "dedup_expr"},
    alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
@ToString
public class SignalTrigger {
  private Map<String, SignalTriggerEntry> definitions; // key is the signal name

  /**
   * Param definitions to inject along the signal trigger. Signal trigger initiator has the
   * evaluated param values.
   */
  @Nullable private Map<String, ParamDefinition> params;

  /**
   * SEL expression to evaluate matched signal instances and return a boolean value. If true,
   * trigger the workflow run. Otherwise, skip it. But the matched signal instances are still
   * considered as consumed. It can use the params defined in the signal trigger.
   */
  @Nullable private String condition;

  /**
   * SEL expression for deduplication and return a String value. This value is used to generate a
   * UUID for the request id. If the same request id is used to start a workflow instance, it will
   * not start a new workflow instance.
   */
  @Nullable private String dedupExpr;

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPropertyOrder(
      value = {"match_params", "join_keys"},
      alphabetic = true)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Data
  @ToString
  public static class SignalTriggerEntry {
    /**
     * matchParams key is param name for matching its value. It has to be static and cannot be a
     * parameter.
     */
    @Nullable private Map<String, SignalMatchParam> matchParams;

    /**
     * Similar to SQL JOIN, joinKeys is the join condition on the list of signal param names with
     * the same value. It must be the same size for all signal trigger entries.
     */
    @Nullable private String[] joinKeys;
  }
}
