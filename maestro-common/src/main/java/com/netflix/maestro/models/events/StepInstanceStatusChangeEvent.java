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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.instance.StepInstance;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Step instance change event schema. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "step_attempt_id",
      "workflow_uuid",
      "step_uuid",
      "correlation_id",
      "step_instance_id",
      "cluster_name",
      "status_change_records",
      "sync_time",
      "send_time",
    },
    alphabetic = true)
@JsonDeserialize(builder = StepInstanceStatusChangeEvent.StepInstanceStatusChangeEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class StepInstanceStatusChangeEvent implements MaestroEvent {
  @NotNull private final String workflowId;

  @Min(1)
  private final long workflowInstanceId;

  @Min(1)
  private final long workflowRunId;

  @NotNull private final String stepId;

  @Min(1)
  private final long stepAttemptId;

  @NotNull private final String workflowUuid;
  @NotNull private final String stepUuid; // internal UUID to identify this step run
  @NotNull private final String correlationId; // default to the workflow instance uuid

  @Min(1)
  private final long stepInstanceId; // step sequence number.

  @NotNull private final String clusterName;

  @Valid @NotEmpty
  private final List<StatusChangeRecord> statusChangeRecords; // a batch of status change records

  @Valid @NotNull private final long syncTime; // the moment that the event is synced internally
  @Valid @NotNull private final long sendTime; // the moment that the event is sent externally

  @Override
  public Type getType() {
    return Type.STEP_INSTANCE_STATUS_CHANGE_EVENT;
  }

  /** Step instance status change record. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {"old_status", "new_status", "event_time"},
      alphabetic = true)
  @Data
  public static class StatusChangeRecord {
    @Valid @NotNull private StepInstance.Status oldStatus;
    @Valid @NotNull private StepInstance.Status newStatus;
    @Valid @NotNull private long eventTime; // the moment that the status change happened
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  public static final class StepInstanceStatusChangeEventBuilder {}
}
