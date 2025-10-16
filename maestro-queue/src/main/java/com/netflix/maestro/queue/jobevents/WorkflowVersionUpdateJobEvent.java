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
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.WorkflowActivationChangeEvent;
import com.netflix.maestro.models.events.WorkflowDeactivationChangeEvent;
import com.netflix.maestro.models.events.WorkflowDefinitionChangeEvent;
import com.netflix.maestro.models.events.WorkflowPropertiesChangeEvent;
import com.netflix.maestro.models.events.WorkflowVersionChangeEvent;
import com.netflix.maestro.utils.ObjectHelper;
import lombok.Data;

/** Workflow definition pending event schema. It will be persisted to DB and sent internally. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "author",
      "workflow_name",
      "version_id",
      "snapshot_id",
      "current_active_version",
      "previous_active_version",
      "log",
      "event_time",
      "sync_time"
    },
    alphabetic = true)
@Data
public class WorkflowVersionUpdateJobEvent implements MaestroJobEvent {
  private static final String LOG_TEMPLATE =
      "Created a new workflow version [%s] for workflow id [%s]";

  private String workflowId;
  private User author;

  private String workflowName;

  @Nullable private Long versionId;
  @Nullable private Long snapshotId;

  @Nullable private Long currentActiveVersion;
  @Nullable private Long previousActiveVersion;

  private String log;

  private long eventTime; // the moment that the status change happened
  private long syncTime; // the moment that the event is synced internally

  /**
   * Static method to create an WorkflowDefinitionPendingEvent from WorkflowDefinition.
   *
   * @param definition workflow definition from API
   * @return a workflow definition change event
   */
  @SuppressWarnings("PMD.NullAssignment")
  public static WorkflowVersionUpdateJobEvent create(
      WorkflowDefinition definition, PropertiesSnapshot snapshot, long activeVersionId) {
    WorkflowVersionUpdateJobEvent event = new WorkflowVersionUpdateJobEvent();
    event.log =
        String.format(
            LOG_TEMPLATE,
            definition.getMetadata().getWorkflowVersionId(),
            definition.getWorkflow().getId());
    event.workflowId = definition.getWorkflow().getId();
    event.workflowName = definition.getWorkflow().getWorkflowNameOrDefault();
    event.author = definition.getMetadata().getVersionAuthor();
    event.versionId = definition.getMetadata().getWorkflowVersionId();
    event.snapshotId = snapshot == null ? null : snapshot.getCreateTime();
    event.eventTime = definition.getMetadata().getCreateTime();
    event.syncTime = System.currentTimeMillis();
    if (definition.getIsActive()) {
      event.currentActiveVersion = definition.getMetadata().getWorkflowVersionId();
    } else {
      event.currentActiveVersion = activeVersionId;
    }
    event.previousActiveVersion = activeVersionId;
    return event;
  }

  /**
   * Static method to create an WorkflowDefinitionChangeEvent from a log string.
   *
   * @return a workflow definition change event
   */
  public static WorkflowVersionUpdateJobEvent create(
      String workflowId, PropertiesSnapshot snapshot) {
    WorkflowVersionUpdateJobEvent event = new WorkflowVersionUpdateJobEvent();
    event.log = "Update the properties for workflow: " + workflowId;
    event.workflowId = workflowId;
    event.author = snapshot.getAuthor();
    event.snapshotId = snapshot.getCreateTime();
    event.eventTime = System.currentTimeMillis();
    event.syncTime = System.currentTimeMillis();
    return event;
  }

  /**
   * Static method to create an WorkflowDefinitionChangeEvent from a log string.
   *
   * @param log log string
   * @return a workflow definition change event
   */
  public static WorkflowVersionUpdateJobEvent create(
      String workflowId, Long curActiveId, Long prevActiveId, User author, String log) {
    WorkflowVersionUpdateJobEvent event = new WorkflowVersionUpdateJobEvent();
    event.log = log;
    event.workflowId = workflowId;
    event.author = author;
    event.currentActiveVersion = curActiveId;
    event.previousActiveVersion = prevActiveId;
    event.eventTime = System.currentTimeMillis();
    event.syncTime = System.currentTimeMillis();
    return event;
  }

  @Override
  public Type getType() {
    return Type.WORKFLOW_VERSION_UPDATE;
  }

  /**
   * Create one of WorkflowChangeEvent types from {@link WorkflowVersionUpdateJobEvent}.
   *
   * @param clusterName workflow definition cluster name
   * @return a workflow change event
   */
  public MaestroEvent toMaestroEvent(String clusterName) {
    if (versionId != null) {
      if (snapshotId != null) {
        return WorkflowDefinitionChangeEvent.builder()
            .workflowId(workflowId)
            .workflowName(workflowName)
            .author(author)
            .versionId(versionId)
            .snapshotId(snapshotId)
            .currentActiveVersionId(ObjectHelper.valueOrDefault(currentActiveVersion, 0L))
            .previousActiveVersionId(ObjectHelper.valueOrDefault(previousActiveVersion, 0L))
            .clusterName(clusterName)
            .eventTime(eventTime)
            .syncTime(syncTime)
            .sendTime(System.currentTimeMillis())
            .build();
      } else {
        return WorkflowVersionChangeEvent.builder()
            .workflowId(workflowId)
            .workflowName(workflowName)
            .author(author)
            .versionId(versionId)
            .currentActiveVersionId(ObjectHelper.valueOrDefault(currentActiveVersion, 0L))
            .previousActiveVersionId(ObjectHelper.valueOrDefault(previousActiveVersion, 0L))
            .clusterName(clusterName)
            .eventTime(eventTime)
            .syncTime(syncTime)
            .sendTime(System.currentTimeMillis())
            .build();
      }
    } else if (snapshotId != null) {
      return WorkflowPropertiesChangeEvent.builder()
          .workflowId(workflowId)
          .author(author)
          .snapshotId(snapshotId)
          .clusterName(clusterName)
          .eventTime(eventTime)
          .syncTime(syncTime)
          .sendTime(System.currentTimeMillis())
          .build();
    } else if (currentActiveVersion != null) {
      return WorkflowActivationChangeEvent.builder()
          .workflowId(workflowId)
          .author(author)
          .currentActiveVersionId(currentActiveVersion)
          .previousActiveVersionId(previousActiveVersion)
          .clusterName(clusterName)
          .eventTime(eventTime)
          .syncTime(syncTime)
          .sendTime(System.currentTimeMillis())
          .build();
    } else {
      return WorkflowDeactivationChangeEvent.builder()
          .workflowId(workflowId)
          .author(author)
          .previousActiveVersionId(previousActiveVersion)
          .clusterName(clusterName)
          .eventTime(eventTime)
          .syncTime(syncTime)
          .sendTime(System.currentTimeMillis())
          .build();
    }
  }
}
