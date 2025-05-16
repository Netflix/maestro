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
package com.netflix.maestro.models.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.validations.TagListConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Workflow instance status change event schema. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "workflow_uuid",
      "correlation_id",
      "depth",
      "workflow_name",
      "cluster_name",
      "tags",
      "old_status",
      "new_status",
      "event_time",
      "sync_time",
      "send_time"
    },
    alphabetic = true)
@JsonDeserialize(
    builder = WorkflowInstanceStatusChangeEvent.WorkflowInstanceStatusChangeEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class WorkflowInstanceStatusChangeEvent implements MaestroEvent {
  @Valid @NotNull private final String workflowId;
  @Valid @NotNull private final String workflowName;

  @Valid
  @Min(1)
  private final long workflowInstanceId;

  @Valid
  @Min(1)
  private final long workflowRunId;

  @NotNull private final String workflowUuid;
  @NotNull private final String correlationId;
  private final int depth;

  @NotNull private final String clusterName;

  @Valid @TagListConstraint private final TagList tags;

  @Valid @NotNull private final WorkflowInstance.Status oldStatus;
  @Valid @NotNull private final WorkflowInstance.Status newStatus;
  @Valid @NotNull private final long eventTime; // the moment that the status change happened
  @Valid @NotNull private final long syncTime; // the moment that the event is synced internally
  @Valid @NotNull private final long sendTime; // the moment that the event is sent externally

  @Override
  public Type getType() {
    return Type.WORKFLOW_INSTANCE_STATUS_CHANGE_EVENT;
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class WorkflowInstanceStatusChangeEventBuilder {}
}
