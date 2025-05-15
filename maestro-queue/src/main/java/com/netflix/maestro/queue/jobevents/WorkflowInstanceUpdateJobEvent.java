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
package com.netflix.maestro.queue.jobevents;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.events.WorkflowInstanceStatusChangeEvent;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;

/** Workflow instance pending change event schema. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "workflow_name", "change_records", "sync_time"},
    alphabetic = true)
@Data
public class WorkflowInstanceUpdateJobEvent implements MaestroJobEvent {
  private String workflowId;
  private String workflowName;
  private List<WorkflowInstanceChangeRecord> changeRecords;
  private long syncTime; // the moment that the event is synced internally

  /** Workflow instance status change record. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder(
      value = {
        "workflow_instance_id",
        "workflow_run_id",
        "workflow_uuid",
        "correlation_id",
        "depth",
        "parent",
        "group_info",
        "tags",
        "old_status",
        "new_status",
        "event_time"
      },
      alphabetic = true)
  @Data
  public static class WorkflowInstanceChangeRecord {
    private long workflowInstanceId;
    private long workflowRunId;

    private String workflowUuid;
    private String correlationId; // default to the workflow instance uuid

    private int depth;
    private UpstreamInitiator.Info parent;
    private long groupInfo;

    private TagList tags;

    private WorkflowInstance.Status oldStatus;
    private WorkflowInstance.Status newStatus;
    private long eventTime; // the moment that the status change happened

    private static WorkflowInstanceChangeRecord fromInstance(
        WorkflowInstance instance, WorkflowInstance.Status status, long markTime) {
      WorkflowInstanceChangeRecord changeRecord = new WorkflowInstanceChangeRecord();
      changeRecord.workflowInstanceId = instance.getWorkflowInstanceId();
      changeRecord.workflowRunId = instance.getWorkflowRunId();
      changeRecord.workflowUuid = instance.getWorkflowUuid();
      changeRecord.correlationId = instance.getCorrelationId();
      changeRecord.depth = instance.getInitiator().getDepth();
      changeRecord.parent = instance.getInitiator().getParent();
      changeRecord.groupInfo = instance.getGroupInfo();
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
   * @param oldStatus old status
   * @param newStatus new status
   * @return status pending change event
   */
  public static WorkflowInstanceUpdateJobEvent create(
      String workflowId,
      String workflowName,
      long workflowInstanceId,
      long workflowRunId,
      String workflowUuid,
      String correlationId,
      Initiator initiator,
      long groupInfo,
      TagList tags,
      WorkflowInstance.Status oldStatus,
      WorkflowInstance.Status newStatus,
      long markTime) {
    WorkflowInstanceUpdateJobEvent event = new WorkflowInstanceUpdateJobEvent();
    event.workflowId = workflowId;
    event.workflowName = workflowName;
    WorkflowInstanceChangeRecord changeRecord = new WorkflowInstanceChangeRecord();
    changeRecord.workflowInstanceId = workflowInstanceId;
    changeRecord.workflowRunId = workflowRunId;
    changeRecord.workflowUuid = workflowUuid;
    changeRecord.correlationId = correlationId;
    changeRecord.depth = initiator.getDepth();
    changeRecord.parent = initiator.getParent();
    changeRecord.groupInfo = groupInfo;
    changeRecord.tags = tags;
    changeRecord.oldStatus = oldStatus;
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
    event.workflowName = instance.getRuntimeWorkflow().getWorkflowNameOrDefault();
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
      List<WorkflowInstance> instances, WorkflowInstance.Status newStatus, long markTime) {
    WorkflowInstanceUpdateJobEvent event = new WorkflowInstanceUpdateJobEvent();
    event.workflowId = instances.getFirst().getWorkflowId();
    event.workflowName = instances.getFirst().getRuntimeWorkflow().getWorkflowNameOrDefault();
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
    return Type.WORKFLOW_INSTANCE_UPDATE;
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
