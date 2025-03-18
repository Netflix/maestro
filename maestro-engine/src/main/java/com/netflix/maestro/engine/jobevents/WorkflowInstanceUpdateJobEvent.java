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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.events.WorkflowInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.validations.TagListConstraint;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;

/** Workflow instance pending change event schema. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "workflow_name", "change_records", "sync_time"},
    alphabetic = true)
@Data
public class WorkflowInstanceUpdateJobEvent implements MaestroJobEvent {
  @Valid @NotNull private String workflowId;
  @Valid @NotNull private String workflowName;
  @NotEmpty private List<WorkflowInstanceChangeRecord> changeRecords;
  @Valid @NotNull private long syncTime; // the moment that the event is synced internally

  /** Workflow instance status change record. */
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "workflow_instance_id",
        "workflow_run_id",
        "workflow_uuid",
        "correlation_id",
        "depth",
        "tags",
        "old_status",
        "new_status",
        "event_time"
      },
      alphabetic = true)
  @Data
  public static class WorkflowInstanceChangeRecord {
    @Valid
    @Min(1)
    private long workflowInstanceId;

    @Valid
    @Min(1)
    private long workflowRunId;

    @NotNull private String workflowUuid;
    @NotNull private String correlationId; // default to the workflow instance uuid

    private int depth;

    @Valid @TagListConstraint private TagList tags;

    @Valid @NotNull private WorkflowInstance.Status oldStatus;
    @Valid @NotNull private WorkflowInstance.Status newStatus;
    @Valid @NotNull private long eventTime; // the moment that the status change happened

    static WorkflowInstanceChangeRecord fromInstance(
        WorkflowInstance instance, WorkflowInstance.Status status, long markTime) {
      WorkflowInstanceChangeRecord changeRecord = new WorkflowInstanceChangeRecord();
      changeRecord.workflowInstanceId = instance.getWorkflowInstanceId();
      changeRecord.workflowRunId = instance.getWorkflowRunId();
      changeRecord.workflowUuid = instance.getWorkflowUuid();
      changeRecord.correlationId = instance.getCorrelationId();
      changeRecord.depth = instance.getInitiator().getDepth();
      changeRecord.tags = instance.getRuntimeWorkflow().getTags();
      changeRecord.oldStatus = instance.getStatus();
      changeRecord.newStatus = status;
      changeRecord.eventTime = markTime;
      return changeRecord;
    }
  }

  /**
   * Static method to create an WorkflowInstanceUpdateJobEvent.
   *
   * @param runtimeSummary old status
   * @param newStatus new status
   * @return status pending change event
   */
  public static WorkflowInstanceUpdateJobEvent create(
      WorkflowSummary summary,
      WorkflowRuntimeSummary runtimeSummary,
      WorkflowInstance.Status newStatus,
      long markTime) {
    WorkflowInstanceUpdateJobEvent event = new WorkflowInstanceUpdateJobEvent();
    event.workflowId = summary.getWorkflowId();
    event.workflowName = summary.getWorkflowName();
    WorkflowInstanceChangeRecord changeRecord = new WorkflowInstanceChangeRecord();
    changeRecord.workflowInstanceId = summary.getWorkflowInstanceId();
    changeRecord.workflowRunId = summary.getWorkflowRunId();
    changeRecord.workflowUuid = summary.getWorkflowUuid();
    changeRecord.correlationId = summary.getCorrelationId();
    changeRecord.depth = summary.getInitiator().getDepth();
    changeRecord.tags = summary.getTags();
    changeRecord.oldStatus = runtimeSummary.getInstanceStatus();
    changeRecord.newStatus = newStatus;
    changeRecord.eventTime = markTime;
    event.changeRecords = Collections.singletonList(changeRecord);
    event.syncTime = System.currentTimeMillis();
    return event;
  }

  /**
   * Static method to create an WorkflowInstanceUpdateJobEvent.
   *
   * @param instance Workflow instance
   * @param newStatus new status
   * @return status pending change event
   */
  public static WorkflowInstanceUpdateJobEvent create(
      WorkflowInstance instance, WorkflowInstance.Status newStatus, long markTime) {
    WorkflowInstanceUpdateJobEvent event = new WorkflowInstanceUpdateJobEvent();
    event.workflowId = instance.getWorkflowId();
    event.workflowName = WorkflowHelper.getWorkflowNameOrDefault(instance.getRuntimeWorkflow());
    event.changeRecords =
        Collections.singletonList(
            WorkflowInstanceChangeRecord.fromInstance(instance, newStatus, markTime));
    event.syncTime = System.currentTimeMillis();
    return event;
  }

  /**
   * Static method to create an WorkflowInstanceUpdateJobEvent.
   *
   * @param instances Workflow instances
   * @param newStatus new status
   * @return status pending change event
   */
  public static WorkflowInstanceUpdateJobEvent create(
      @NotEmpty List<WorkflowInstance> instances,
      WorkflowInstance.Status newStatus,
      long markTime) {
    WorkflowInstanceUpdateJobEvent event = new WorkflowInstanceUpdateJobEvent();
    event.workflowId = instances.getFirst().getWorkflowId();
    event.workflowName =
        WorkflowHelper.getWorkflowNameOrDefault(instances.getFirst().getRuntimeWorkflow());
    event.changeRecords =
        instances.stream()
            .map(
                instance ->
                    WorkflowInstanceChangeRecord.fromInstance(instance, newStatus, markTime))
            .collect(Collectors.toList());
    event.syncTime = System.currentTimeMillis();
    return event;
  }

  @Override
  public Type getType() {
    return Type.WORKFLOW_INSTANCE_UPDATE_JOB_EVENT;
  }

  /**
   * Create a WorkflowInstanceStatusChangeEvent stream.
   *
   * @param clusterName cluster name
   * @return status change event
   */
  public Stream<WorkflowInstanceStatusChangeEvent> toMaestroEventStream(String clusterName) {
    return changeRecords.stream()
        .map(
            changeRecord ->
                WorkflowInstanceStatusChangeEvent.builder()
                    .workflowId(workflowId)
                    .workflowName(workflowName)
                    .workflowInstanceId(changeRecord.workflowInstanceId)
                    .workflowRunId(changeRecord.workflowRunId)
                    .workflowUuid(changeRecord.workflowUuid)
                    .correlationId(changeRecord.correlationId)
                    .depth(changeRecord.depth)
                    .clusterName(clusterName)
                    .tags(changeRecord.tags)
                    .oldStatus(changeRecord.oldStatus)
                    .newStatus(changeRecord.newStatus)
                    .eventTime(changeRecord.eventTime)
                    .syncTime(syncTime)
                    .sendTime(System.currentTimeMillis())
                    .build());
  }
}
