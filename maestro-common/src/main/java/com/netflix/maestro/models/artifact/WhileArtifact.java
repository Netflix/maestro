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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import java.util.List;
import java.util.Map;
import lombok.Data;

/** Foreach artifact to store foreach step loop stats overview at runtime. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "loop_workflow_id",
      "loop_identity",
      "loop_run_id",
      "run_policy",
      "run_id",
      "first_iteration",
      "last_iteration",
      "last_status",
      "last_overview",
      "rollup",
      "loop_param_values",
    },
    alphabetic = true)
@Data
public class WhileArtifact implements Artifact {
  private String loopWorkflowId; // while loop inline workflow id
  private String loopIdentity; // while loop inline workflow identity
  private long loopRunId; // while loop inline workflow run id

  private RunPolicy runPolicy; // while step run policy used to run while iteration
  private long runId; // while step run id, always match upstream workflow

  private long firstIteration; // this is the expected first while loop iteration id
  private long lastIteration; // this is the launched last while loop iteration id

  @Nullable
  private WorkflowInstance.Status lastStatus; // this is the status of the last while loop iteration

  @Nullable private WorkflowRuntimeOverview lastOverview; // the last while loop iteration overview

  private WorkflowRollupOverview rollup; // rollup of all iterations except the last iteration
  private Map<String, List<Object>> loopParamValues; // loop param values for each iteration

  @JsonIgnore
  @Override
  public WhileArtifact asWhile() {
    return this;
  }

  @Override
  public Type getType() {
    return Type.WHILE;
  }

  /** check if this while is a fresh run. */
  @JsonIgnore
  public boolean isFreshRun() {
    return runPolicy == null || runPolicy.isFreshRun();
  }

  /**
   * Get merged rollup info from complete iterations and running iteration aggregated from run1 to
   * current run.
   */
  @JsonIgnore
  public WorkflowRollupOverview getOverallRollup() {
    WorkflowRollupOverview overall = new WorkflowRollupOverview();
    if (rollup != null) {
      overall.aggregate(rollup);
    }
    if (lastOverview != null) {
      overall.aggregate(lastOverview.getRollupOverview());
    }
    return overall;
  }
}
