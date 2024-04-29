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
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Maestro internal job event to terminate a small batch of workflow instances (i.e. 1) for a given
 * workflow id. If instanceRunUuids is empty, no instance needs to stop. It also includes a
 * run_after workflow instance, which will run after all instanceRunUuids are terminated. This job
 * event is used within a DB transaction for applying first_only and last_only run strategy.
 * Different from {@link TerminateInstancesJobEvent}, it cannot use a message key to dedup because
 * of the race condition between event publishing and the DB transaction.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "instance_run_uuids", "action", "user", "reason", "run_after"},
    alphabetic = true)
@Data
public class TerminateThenRunInstanceJobEvent implements MaestroJobEvent {
  @NotNull private String workflowId;
  @Valid @NotNull private List<InstanceRunUuid> instanceRunUuids;
  @NotNull private Actions.WorkflowInstanceAction action;
  @Valid @NotNull private User user;
  @NotNull private String reason;
  @Valid @NotNull private InstanceRunUuid runAfter;

  /** Static method to initialize a {@link TerminateThenRunInstanceJobEvent}. */
  public static TerminateThenRunInstanceJobEvent init(
      String workflowId, Actions.WorkflowInstanceAction action, User caller, String reason) {
    TerminateThenRunInstanceJobEvent job = new TerminateThenRunInstanceJobEvent();
    job.workflowId = workflowId;
    job.instanceRunUuids = new ArrayList<>();
    job.action = action;
    job.user = caller;
    job.reason = reason;
    return job;
  }

  /** Add a workflow run (instance id, run id, uuid) to the job event. */
  @JsonIgnore
  public void addOneRun(InstanceRunUuid iru) {
    this.instanceRunUuids.add(iru);
  }

  /**
   * Add a run_after workflow run (instance id, run id, uuid) to the terminate job event. It will
   * run after terminating all instances.
   */
  @JsonIgnore
  public void addRunAfter(long instanceId, long runId, String uuid) {
    this.runAfter = new InstanceRunUuid(instanceId, runId, uuid);
  }

  @Override
  public Type getType() {
    return Type.TERMINATE_THEN_RUN_JOB_EVENT;
  }
}
