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
package com.netflix.maestro.extensions.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Map;
import lombok.Data;
import lombok.ToString;

/** Represents stepIteration data model in the context of foreach flattening. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instanceId",
      "workflow_runId",
      "step_id",
      "iteration_rank",
      "step_attempt_seq",
      "loop_params",
      "step_runtime_state"
    },
    alphabetic = true)
@Data
@ToString
public class StepIteration {
  private String workflowId;
  private long workflowInstanceId;
  private long workflowRunId;
  private String stepId;
  private String iterationRank;
  private String stepAttemptSeq;
  @Nullable private Map<String, String> loopParams;
  private StepRuntimeState stepRuntimeState;

  public static StepIteration create(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      String stepId,
      String iterationRank,
      String stepAttemptSeq,
      Map<String, String> loopParams,
      StepRuntimeState stepRuntimeState) {
    StepIteration ret = new StepIteration();
    ret.setWorkflowId(workflowId);
    ret.setWorkflowInstanceId(workflowInstanceId);
    ret.setWorkflowRunId(workflowRunId);
    ret.setStepId(stepId);
    ret.setIterationRank(iterationRank);
    ret.setStepAttemptSeq(stepAttemptSeq);
    ret.setLoopParams(loopParams);
    ret.setStepRuntimeState(stepRuntimeState);
    return ret;
  }
}
