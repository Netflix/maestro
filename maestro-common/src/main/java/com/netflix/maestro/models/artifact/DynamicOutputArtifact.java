/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.models.artifact;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.List;
import javax.validation.Valid;
import lombok.Data;

/** Signals artifact to store dynamic signals generated at step runtime. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"signal_outputs", "info"},
    alphabetic = true)
@Data
public class DynamicOutputArtifact implements Artifact {
  @Valid @Nullable private List<MapParameter> signalOutputs;
  @Nullable private TimelineLogEvent info;

  @JsonIgnore
  @Override
  public DynamicOutputArtifact asDynamicOutput() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.DYNAMIC_OUTPUT;
  }
}
