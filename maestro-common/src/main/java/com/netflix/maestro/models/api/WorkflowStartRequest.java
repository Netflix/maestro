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
package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.validations.RunParamsConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Data;

/**
 * Workflow start request to start a new workflow instance for a specific workflow. request_id is
 * used for deduplication. It is possible to provide a correlationId at start time to associate a
 * group of workflow instances together. It is currently only for internal usage and not a public
 * facing field.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "initiator",
      "request_time",
      "request_id",
      "runtime_tags",
      "run_params",
      "artifacts",
      "persist_failed_run"
    },
    alphabetic = true)
@Data
public class WorkflowStartRequest {
  @Valid @NotNull private Initiator initiator;

  // The time that the start is requested. By default, it is request received time.
  @Valid @NotNull private long requestTime = System.currentTimeMillis();

  private UUID requestId;

  @Valid private List<Tag> runtimeTags; // at start time, can add tags to the instance.

  // for start, the validation should make sure some params cannot be mutated
  @Valid @RunParamsConstraint
  private Map<String, ParamDefinition> runParams; // runtime parameter overrides

  /** set runParams. */
  public void setRunParams(Map<String, ParamDefinition> input) {
    this.runParams = ParamDefinition.preprocessDefinitionParams(input);
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Valid
  private Map<String, Artifact> artifacts;

  private boolean persistFailedRun; // default is false
}
