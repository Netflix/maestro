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
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Map;
import lombok.Data;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "group_info",
      "step_id",
      "step_attempt_id",
      "step_status",
      "step_type",
      "step_action",
      "workflow_action",
      "instance_run_ids",
      "action_code",
      "entity_type"
    },
    alphabetic = true)
@Data
public class InstanceActionJobEvent implements MaestroJobEvent {
  private String workflowId;
  private long workflowInstanceId;
  private long workflowRunId;
  private long groupInfo; // used to derive the group id for the step instance

  @Nullable private String stepId;
  @Nullable private String stepAttemptId;
  @Nullable private Actions.StepInstanceAction stepAction;
  @Nullable private Actions.WorkflowInstanceAction workflowAction;
  @Nullable private Map<Long, Long> instanceRunIds;
  @Nullable private Integer actionCode;
  private EntityType entityType;

  @Override
  public Type getType() {
    return Type.INSTANCE_ACTION;
  }

  /** It is used to describe on which entity the requested action is for. */
  public enum EntityType {
    /** The workflow level entity. */
    WORKFLOW,
    /** The step level entity. */
    STEP,
    /** The flow level entity. */
    FLOW,
    /** The task level entity. */
    TASK
  }

  /**
   * Static method to create an InstanceActionJobEvent for a step action.
   *
   * @param stepInstance step instance
   * @param action step action
   * @return a step instance action event object
   */
  public static InstanceActionJobEvent create(
      StepInstance stepInstance, Actions.StepInstanceAction action) {
    InstanceActionJobEvent event = new InstanceActionJobEvent();
    event.workflowId = stepInstance.getWorkflowId();
    event.workflowInstanceId = stepInstance.getWorkflowInstanceId();
    event.workflowRunId = stepInstance.getWorkflowRunId();
    event.stepId = stepInstance.getStepId();
    event.stepAction = action;
    event.entityType = EntityType.STEP;
    event.groupInfo = stepInstance.getGroupInfo();

    if (stepInstance.getStepAttemptId() > 0) {
      event.stepAttemptId = String.valueOf(stepInstance.getStepAttemptId());
    } else {
      event.stepAttemptId = Constants.LATEST_INSTANCE_RUN;
    }
    return event;
  }

  /**
   * Static method to create an InstanceActionJobEvent for a workflow action.
   *
   * @param instance workflow instance
   * @param action workflow action
   * @return a workflow instance action event object
   */
  public static InstanceActionJobEvent create(
      WorkflowInstance instance, Actions.WorkflowInstanceAction action) {
    InstanceActionJobEvent event = new InstanceActionJobEvent();
    event.workflowId = instance.getWorkflowId();
    event.workflowInstanceId = instance.getWorkflowInstanceId();
    event.workflowRunId = instance.getWorkflowRunId();
    event.groupInfo = instance.getGroupInfo();
    event.workflowAction = action;
    event.entityType = EntityType.WORKFLOW;
    return event;
  }

  /**
   * Static method to create an InstanceActionJobEvent for a flow action. It expected to stay only
   * in the memory and does not need any guarantee.
   *
   * @param workflowId the workflow id to stop
   * @param instanceRunIds instance run id map for the given workflow to stop
   * @return a flow action event object
   */
  public static InstanceActionJobEvent create(
      String workflowId, long groupInfo, Map<Long, Long> instanceRunIds) {
    InstanceActionJobEvent event = new InstanceActionJobEvent();
    event.workflowId = workflowId;
    event.groupInfo = groupInfo;
    event.instanceRunIds = instanceRunIds;
    event.entityType = EntityType.FLOW;
    return event;
  }

  /**
   * Static method to create an InstanceActionJobEvent for a task action. It expected to stay only
   * in the memory and does not need any guarantee.
   *
   * @param groupId the group id of the flow
   * @param flowRef the flow reference id
   * @param taskRef the task reference id
   * @param code action code
   * @return a task action event object
   */
  public static InstanceActionJobEvent create(
      long groupId, String flowRef, String taskRef, int code) {
    InstanceActionJobEvent event = new InstanceActionJobEvent();
    event.workflowId = flowRef;
    event.groupInfo = groupId;
    event.stepId = taskRef;
    event.actionCode = code;
    event.entityType = EntityType.TASK;
    return event;
  }

  @JsonIgnore
  public String getIdentity() {
    return switch (entityType) {
      case WORKFLOW ->
          String.format(
              "[%s][%s][%s][%s][%s]",
              entityType.name(),
              workflowId,
              workflowInstanceId,
              workflowRunId,
              workflowAction.name());
      case STEP ->
          String.format(
              "[%s][%s][%s][%s][%s][%s][%s]",
              entityType.name(),
              workflowId,
              workflowInstanceId,
              workflowRunId,
              stepId,
              stepAttemptId == null ? Constants.LATEST_INSTANCE_RUN : stepAttemptId,
              stepAction.name());
      case FLOW ->
          String.format("[%s][%s]%s", entityType.name(), workflowId, instanceRunIds.size());
      case TASK ->
          String.format("[%s][%s][%s][%s]", entityType.name(), workflowId, stepId, actionCode);
    };
  }
}
