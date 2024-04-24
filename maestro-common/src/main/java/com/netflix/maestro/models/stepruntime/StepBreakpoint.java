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
package com.netflix.maestro.models.stepruntime;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.definition.User;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * This is StepBreakpoint data model to represent a single breakpoint. Following layered breakpoints
 * are supported:
 *
 * <p><wfId,stepId,MATCH_ALL_INSTANCES,MATCH_ALL_RUNS,MATCH_ALL_ATTEMPTS> - generic breakpoint for
 * step definition. It will pause all step attempts for any instance/run.
 *
 * <p><wfId,stepId,specific_instance,MATCH_ALL_RUNS,MATCH_ALL_ATTEMPTS> - breakpoint for step
 * instance. It will pause each step attempt in a particular instance for all runs.
 *
 * <p><wfId,stepId,specific_instance,specific_run,MATCH_ALL_ATTEMPTS> - breakpoint for step instance
 * runs. It will pause all step attempts in a particular instance and run.
 *
 * <p><wfId,stepId,specific_instance,specific_run,specific_step_attempt> - specific breakpoint for
 * step instance attempt. It will pause a particular step attempt.
 */
@JsonDeserialize(builder = StepBreakpoint.StepBreakpointBuilder.class)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "step_id",
      "workflow_version_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_attempt_id",
      "create_time",
      "created_by",
    },
    alphabetic = true)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class StepBreakpoint {
  @NotNull private final String workflowId;
  @NotNull private final String stepId;
  private final Long workflowVersionId;
  private final Long workflowInstanceId;
  private final Long workflowRunId;
  private final Long stepAttemptId;
  private final Long createTime;
  private final User createdBy;

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  public static final class StepBreakpointBuilder {}
}
