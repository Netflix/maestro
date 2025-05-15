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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.timeline.TimelineEvent;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * step instance action request to stop/kill/pause/resume/skip/bypass-step-dependencies a step
 * instance for a specific workflow instance.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "step_attempt_id",
      "step_runtimes_state",
      "timeline_event"
    },
    alphabetic = true)
@JsonDeserialize(builder = StepInstanceActionResponse.StepInstanceActionResponseBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class StepInstanceActionResponse {
  private final String workflowId;
  private final long workflowInstanceId;
  private final long workflowRunId;
  private final String stepId;
  private final Long stepAttemptId;
  private final StepRuntimeState stepRuntimeState;
  private final TimelineEvent timelineEvent;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class StepInstanceActionResponseBuilder {}
}
