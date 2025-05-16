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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Data Model to represent a paused step attempt due to a breakpoint. */
@JsonDeserialize(builder = PausedStepAttempt.PausedStepAttemptBuilder.class)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_version_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "step_attempt_id",
      "create_time"
    },
    alphabetic = true)
@Builder
@Getter
@EqualsAndHashCode
public class PausedStepAttempt {
  @NotNull private final String workflowId;
  @NotNull private final String stepId;
  private final long workflowVersionId;
  private final long workflowInstanceId;
  private final long workflowRunId;
  private final long stepAttemptId;
  private final long createTime;

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class PausedStepAttemptBuilder {}
}
