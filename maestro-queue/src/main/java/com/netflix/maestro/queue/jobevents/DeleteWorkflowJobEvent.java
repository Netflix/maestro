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
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.WorkflowDeletionChangeEvent;
import lombok.Data;

/**
 * Maestro internal job event to DELETE all related data for a given workflow id. It is sent after
 * the workflow basic info is deleted. It requires exactly-once semantics.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "internal_id", "author", "timestamp", "type"},
    alphabetic = true)
@Data
public class DeleteWorkflowJobEvent implements MaestroJobEvent {
  private String workflowId;
  private long internalId;
  private User author;

  // the moment that the event is created.
  private long timestamp = System.currentTimeMillis();

  /** static creator. */
  public static DeleteWorkflowJobEvent create(String workflowId, long internalId, User author) {
    DeleteWorkflowJobEvent jobEvent = new DeleteWorkflowJobEvent();
    jobEvent.setWorkflowId(workflowId);
    jobEvent.setInternalId(internalId);
    jobEvent.setAuthor(author);
    return jobEvent;
  }

  @Override
  public Type getType() {
    return Type.DELETE_WORKFLOW;
  }

  /** Transform it to a maestro event. */
  public MaestroEvent toMaestroEvent(String clusterName) {
    return WorkflowDeletionChangeEvent.builder()
        .workflowId(workflowId)
        .internalId(internalId)
        .author(author)
        .clusterName(clusterName)
        .eventTime(timestamp)
        .syncTime(timestamp)
        .sendTime(System.currentTimeMillis())
        .build();
  }

  @Override
  public String deriveMessageKey() {
    return workflowId + '-' + internalId;
  }
}
