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
package com.netflix.maestro.engine.jobevents;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.StepInstance;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Step instance pending change batch event schema. This is used to publish a batch of pending
 * changes wrapped within a single maestro event.
 */
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
      "pending_records",
      "sync_time"
    },
    alphabetic = true)
@Data
public class StepInstanceUpdateJobEvent implements MaestroJobEvent {
  @NotNull private String workflowId;

  @Min(1)
  private long workflowInstanceId;

  @Min(1)
  private long workflowRunId;

  @NotNull private String stepId;

  @Min(1)
  private long stepAttemptId;

  @NotNull private String workflowUuid;
  @NotNull private String stepUuid; // internal UUID to identify this step run
  @NotNull private String correlationId; // default to the workflow instance uuid

  @Min(1)
  private long stepInstanceId; // step sequence number.

  @NotEmpty private List<StepInstancePendingRecord> pendingRecords;
  @Valid @NotNull private long syncTime; // the moment that the event is synced internally

  @JsonIgnore
  public boolean hasTerminal() {
    return pendingRecords.stream()
        .map(pendingRecord -> pendingRecord.getNewStatus().isTerminal())
        .reduce(false, Boolean::logicalOr);
  }

  /**
   * Static method to create an StepInstancePendingBatchEvent.
   *
   * @param instance step instance
   * @param pendingEvents a list of pending events
   * @return status change event
   */
  public static StepInstanceUpdateJobEvent create(
      StepInstance instance, List<StepInstancePendingRecord> pendingEvents) {
    StepInstanceUpdateJobEvent event = new StepInstanceUpdateJobEvent();
    event.workflowId = instance.getWorkflowId();
    event.workflowInstanceId = instance.getWorkflowInstanceId();
    event.workflowRunId = instance.getWorkflowRunId();
    event.stepId = instance.getStepId();
    event.stepAttemptId = instance.getStepAttemptId();
    event.workflowUuid = instance.getWorkflowUuid();
    event.stepUuid = instance.getStepUuid();
    event.correlationId = instance.getCorrelationId();
    event.stepInstanceId = instance.getStepInstanceId();
    event.pendingRecords = new ArrayList<>(pendingEvents); // make a copy
    event.syncTime = System.currentTimeMillis();
    return event;
  }

  @Override
  public Type getType() {
    return Type.STEP_INSTANCE_UPDATE_JOB_EVENT;
  }

  /** Static method to create StepInstancePendingRecord. */
  public static StepInstancePendingRecord createRecord(
      StepInstance.Status oldStatus, StepInstance.Status newStatus, long markTime) {
    StepInstancePendingRecord record = new StepInstancePendingRecord();
    record.setOldStatus(oldStatus);
    record.setNewStatus(newStatus);
    record.setEventTime(markTime);
    return record;
  }

  /** Step instance pending record schema. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {"old_status", "new_status", "event_time"},
      alphabetic = true)
  @Data
  public static class StepInstancePendingRecord {
    @Valid @NotNull private StepInstance.Status oldStatus;
    @Valid @NotNull private StepInstance.Status newStatus;
    @Valid @NotNull private long eventTime; // the moment that the status change happened

    /**
     * create a {@link StepInstanceStatusChangeEvent.StatusChangeRecord} from {@link
     * StepInstancePendingRecord}.
     *
     * @return status change event
     */
    @JsonIgnore
    public StepInstanceStatusChangeEvent.StatusChangeRecord toMaestroRecord() {
      StepInstanceStatusChangeEvent.StatusChangeRecord record =
          new StepInstanceStatusChangeEvent.StatusChangeRecord();
      record.setOldStatus(this.oldStatus);
      record.setNewStatus(this.newStatus);
      record.setEventTime(this.eventTime);
      return record;
    }
  }

  /**
   * create a StepInstanceStatusChangeEvent from a StepInstanceUpdateJobEvent.
   *
   * @param clusterName cluster name
   * @return status change event
   */
  @JsonIgnore
  public StepInstanceStatusChangeEvent toMaestroEvent(String clusterName) {
    return StepInstanceStatusChangeEvent.builder()
        .workflowId(this.getWorkflowId())
        .workflowInstanceId(this.getWorkflowInstanceId())
        .workflowRunId(this.getWorkflowRunId())
        .stepId(this.getStepId())
        .stepAttemptId(this.getStepAttemptId())
        .workflowUuid(this.getWorkflowUuid())
        .stepUuid(this.getStepUuid())
        .correlationId(this.getCorrelationId())
        .stepInstanceId(this.getStepInstanceId())
        .clusterName(clusterName)
        .statusChangeRecords(
            this.pendingRecords.stream()
                .map(StepInstancePendingRecord::toMaestroRecord)
                .collect(Collectors.toList()))
        .syncTime(this.getSyncTime())
        .sendTime(System.currentTimeMillis())
        .build();
  }
}
