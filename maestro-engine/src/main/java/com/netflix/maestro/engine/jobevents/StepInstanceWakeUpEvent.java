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
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.HashHelper;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "workflow_id",
      "workflow_instance_id",
      "workflow_run_id",
      "step_id",
      "step_attempt_id",
      "step_uuid",
      "step_status",
      "step_type",
      "step_action",
      "workflow_action",
      "entity_type"
    },
    alphabetic = true)
@Data
public class StepInstanceWakeUpEvent implements MaestroJobEvent {
  @NotNull private String workflowId;

  @Min(1)
  private long workflowInstanceId;

  @Min(1)
  private long workflowRunId;

  @Nullable private String stepId;
  @Nullable private String stepAttemptId;
  @Nullable private String stepUuid; // internal UUID to identify this step run
  @Nullable private StepInstance.Status stepStatus;
  @Nullable private StepType stepType;
  @Nullable private Actions.StepInstanceAction stepAction;
  @Nullable private Actions.WorkflowInstanceAction workflowAction;
  @NotNull private EntityType entityType;

  @Override
  public Type getType() {
    return Type.STEP_INSTANCE_WAKE_UP_JOB_EVENT;
  }

  /** It is used to describe on which entity the requested action is for. */
  public enum EntityType {
    /** The workflow level entity. */
    WORKFLOW,
    /** The step level entity. */
    STEP
  }

  /**
   * static method to create a StepInstanceWakeUpEvent.
   *
   * @param stepInstance step instance
   * @param action step action
   * @return a step instance wakeup event object
   */
  public static StepInstanceWakeUpEvent create(StepInstance stepInstance, StepAction action) {
    StepInstanceWakeUpEvent event = new StepInstanceWakeUpEvent();
    event.workflowId = action.getWorkflowId();
    event.workflowInstanceId = action.getWorkflowInstanceId();
    event.workflowRunId = action.getWorkflowRunId();
    event.stepId = action.getStepId();
    event.stepAction = action.getAction();
    event.entityType = EntityType.STEP;
    if (stepInstance.getStepUuid() != null) {
      event.stepUuid = stepInstance.getStepUuid();
    }
    if (stepInstance.getStepAttemptId() > 0) {
      event.stepAttemptId = String.valueOf(stepInstance.getStepAttemptId());
    } else {
      event.stepAttemptId = Constants.LATEST_INSTANCE_RUN;
    }
    if (stepInstance.getDefinition() != null && stepInstance.getDefinition().getType() != null) {
      event.stepType = stepInstance.getDefinition().getType();
    }
    if (stepInstance.getRuntimeState() != null
        && stepInstance.getRuntimeState().getStatus() != null) {
      event.stepStatus = stepInstance.getRuntimeState().getStatus();
    }
    return event;
  }

  /**
   * static method to create a StepInstanceWakeUpEvent.
   *
   * @param instance workflow instance
   * @param action workflow action
   * @return a step instance wakeup event object
   */
  public static StepInstanceWakeUpEvent create(
      WorkflowInstance instance, Actions.WorkflowInstanceAction action) {
    StepInstanceWakeUpEvent event = new StepInstanceWakeUpEvent();
    event.workflowId = instance.getWorkflowId();
    event.workflowInstanceId = instance.getWorkflowInstanceId();
    event.workflowRunId = instance.getWorkflowRunId();
    event.workflowAction = action;
    event.entityType = EntityType.WORKFLOW;
    return event;
  }

  @Override
  public String getMessageKey() {
    if (this.entityType == EntityType.WORKFLOW) {
      return HashHelper.md5(
          String.format(
              "[%s]_[%s]_[%s]_[%s]_[%s]",
              entityType.name(),
              workflowId,
              workflowInstanceId,
              workflowRunId,
              workflowAction.name()));
    }
    return HashHelper.md5(
        String.format(
            "[%s]_[%s]_[%s]_[%s]_[%s]_[%s]_[%s]",
            entityType.name(),
            workflowId,
            workflowInstanceId,
            workflowRunId,
            stepId,
            stepAttemptId == null ? Constants.LATEST_INSTANCE_RUN : stepAttemptId,
            stepAction.name()));
  }
}
