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
import com.netflix.maestro.engine.db.InstanceRunUuid;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.definition.User;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * Maestro internal job event to terminate a batch of workflow instances for a given workflow id. If
 * the field instanceRunUuids is empty, no instance needs to stop. It should not be emitted within a
 * DB transaction. This event is used by users or Maestro to stop a batch of workflow instances for
 * a given workflow id when stopping or killing all.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "instance_run_uuids", "action", "user", "reason"},
    alphabetic = true)
@Data
public class TerminateInstancesJobEvent implements MaestroJobEvent {
  @NotNull private String workflowId;
  @Valid @NotNull private List<InstanceRunUuid> instanceRunUuids;
  @NotNull private Actions.WorkflowInstanceAction action;
  @Valid @NotNull private User user;
  @NotNull private String reason;

  /** Static method to initialize a {@link TerminateInstancesJobEvent}. */
  public static TerminateInstancesJobEvent init(
      String workflowId, Actions.WorkflowInstanceAction action, User caller, String reason) {
    TerminateInstancesJobEvent job = new TerminateInstancesJobEvent();
    job.workflowId = workflowId;
    job.instanceRunUuids = new ArrayList<>();
    job.action = action;
    job.user = caller;
    job.reason = reason;
    return job;
  }

  /** Add a workflow run (instance id, run id, uuid) to the job event. */
  @JsonIgnore
  public void addOneRun(long instanceId, long runId, String uuid) {
    this.instanceRunUuids.add(new InstanceRunUuid(instanceId, runId, uuid));
  }

  /** Return the size of instances. */
  @JsonIgnore
  public int size() {
    return instanceRunUuids == null ? 0 : instanceRunUuids.size();
  }

  @Override
  public Type getType() {
    return Type.TERMINATE_INSTANCES_JOB_EVENT;
  }
}
