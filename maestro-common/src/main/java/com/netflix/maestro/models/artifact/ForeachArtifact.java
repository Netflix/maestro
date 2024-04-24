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
package com.netflix.maestro.models.artifact;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.instance.ForeachAction;
import com.netflix.maestro.models.instance.ForeachStepOverview;
import com.netflix.maestro.models.instance.RunPolicy;
import lombok.Data;

/** Foreach artifact to store foreach step loop stats overview at runtime. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "foreach_workflow_id",
      "foreach_identity",
      "foreach_run_id",
      "run_policy",
      "run_id",
      "ancestor_iteration_count",
      "total_loop_count",
      "next_loop_index",
      "foreach_overview"
    },
    alphabetic = true)
@Data
public class ForeachArtifact implements Artifact {
  private String foreachWorkflowId; // foreach inline workflow id
  private String foreachIdentity; // foreach inline workflow identity
  private long foreachRunId; // foreach inline workflow run id

  private RunPolicy runPolicy; // foreach step run policy used to run foreach iteration
  private long runId; // foreach step run id, always match upstream workflow
  private Long ancestorIterationCount;

  private int totalLoopCount;
  private int nextLoopIndex;
  private ForeachStepOverview foreachOverview;

  @Nullable private ForeachAction pendingAction; // support only a single pending action

  @JsonIgnore
  @Override
  public ForeachArtifact asForeach() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.FOREACH;
  }

  /** check if this foreach is a fresh run. */
  @JsonIgnore
  public boolean isFreshRun() {
    return runPolicy == null || runPolicy.isFreshRun();
  }
}
