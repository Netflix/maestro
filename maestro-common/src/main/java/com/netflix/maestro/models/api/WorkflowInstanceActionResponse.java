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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineEvent;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** workflow instance action response to stop/kill/unblock a workflow instance run. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "workflow_instance_status",
      "info",
      "timeline_event"
    },
    alphabetic = true)
@JsonDeserialize(
    builder = WorkflowInstanceActionResponse.WorkflowInstanceActionResponseBuilder.class)
@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
public class WorkflowInstanceActionResponse {
  private final String workflowId;
  private final long workflowInstanceId;
  private final long workflowRunId;
  private final WorkflowInstance.Status workflowInstanceStatus;
  private final boolean completed;
  private final String info;
  private final TimelineEvent timelineEvent;

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class WorkflowInstanceActionResponseBuilder {}

  /**
   * static method to create a {@link WorkflowInstanceActionResponse} from a {@link
   * WorkflowInstance} and its completed status and event log message.
   */
  @JsonIgnore
  public static WorkflowInstanceActionResponse from(
      WorkflowInstance instance,
      WorkflowInstance.Status toStatus,
      TimelineEvent event,
      boolean completed) {
    WorkflowInstanceActionResponseBuilder builder =
        WorkflowInstanceActionResponse.builder()
            .workflowId(instance.getWorkflowId())
            .workflowInstanceId(instance.getWorkflowInstanceId())
            .workflowRunId(instance.getWorkflowRunId())
            .timelineEvent(event)
            .completed(completed);
    if (completed) {
      builder.workflowInstanceStatus(toStatus);
      builder.info("The action is completed.");
    } else {
      builder.workflowInstanceStatus(instance.getStatus());
      builder.info("The action is accepted and is still running. Please check it again.");
    }
    return builder.build();
  }

  /**
   * static method to create a {@link WorkflowInstanceActionResponse} from a {@link
   * WorkflowInstance} and its updated flag and event log message.
   */
  @JsonIgnore
  public static WorkflowInstanceActionResponse from(
      WorkflowInstance instance, TimelineEvent event, boolean updated) {
    WorkflowInstanceActionResponseBuilder builder =
        WorkflowInstanceActionResponse.builder()
            .workflowId(instance.getWorkflowId())
            .workflowInstanceId(instance.getWorkflowInstanceId())
            .workflowRunId(instance.getWorkflowRunId())
            .workflowInstanceStatus(instance.getStatus())
            .timelineEvent(event)
            .completed(true);
    if (updated) {
      builder.info("The action is done.");
    } else {
      builder.info("The action is done without any operation.");
    }
    return builder.build();
  }
}
