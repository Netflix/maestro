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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.queue.models.InstanceRunUuid;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * Maestro internal job event to terminate a small batch of workflow instances (i.e. 1) for a given
 * workflow id. If instanceRunUuids is empty, no instance needs to stop. It also includes a
 * run_after workflow instance, which will run after all instanceRunUuids are terminated. This job
 * event is used within a DB transaction for applying first_only and last_only run strategy. If
 * runAfter is null, it just terminates the instances without running any new instance.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"workflow_id", "instance_run_uuids", "action", "user", "reason", "run_after"},
    alphabetic = true)
@Data
public class TerminateThenRunJobEvent implements MaestroJobEvent {
  private String workflowId;
  private List<InstanceRunUuid> instanceRunUuids;
  private Actions.WorkflowInstanceAction action;
  private User user;
  private String reason;
  @Nullable private List<InstanceRunUuid> runAfter;
  @JsonIgnore @Nullable private List<InstanceRunUuid> terminating;

  /** Static method to initialize a {@link TerminateThenRunJobEvent}. */
  public static TerminateThenRunJobEvent init(
      String workflowId, Actions.WorkflowInstanceAction action, User caller, String reason) {
    TerminateThenRunJobEvent job = new TerminateThenRunJobEvent();
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

  /** Add a workflow run (instance id, run id, uuid) to the job event. */
  @JsonIgnore
  public void addOneRun(long instanceId, long runId, String uuid) {
    addOneRun(new InstanceRunUuid(instanceId, runId, uuid));
  }

  /** Return the size of instances to terminate. */
  @JsonIgnore
  public int size() {
    return instanceRunUuids == null ? 0 : instanceRunUuids.size();
  }

  /**
   * Add a run_after workflow run (instance id, run id, uuid) to the terminate job event. It will
   * run after terminating all instances.
   */
  @JsonIgnore
  public void addRunAfter(long instanceId, long runId, String uuid) {
    this.runAfter = List.of(new InstanceRunUuid(instanceId, runId, uuid));
  }

  @Override
  public Type getType() {
    return Type.TERMINATE_THEN_RUN;
  }
}
