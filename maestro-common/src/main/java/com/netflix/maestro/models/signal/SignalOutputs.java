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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.List;
import java.util.Map;
import lombok.Data;

/** Signal output. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {"outputs", "info"},
    alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class SignalOutputs {
  private List<SignalOutput> outputs;
  @Nullable private TimelineLogEvent info;

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPropertyOrder(
      value = {"name", "params", "signal_id", "announce_time"},
      alphabetic = true)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Data
  public static class SignalOutput {
    private String name;
    @Nullable private Map<String, SignalParamValue> params;
    @Nullable private Map<String, Object> payload;
    private Long signalId; // set it if the signal id exists
    private Long announceTime; // null if it is dup and announced by others
  }
}
