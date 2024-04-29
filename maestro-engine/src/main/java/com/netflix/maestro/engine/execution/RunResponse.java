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
package com.netflix.maestro.engine.execution;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.api.InstanceRunStatus;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.api.StepInstanceRestartResponse;
import com.netflix.maestro.models.api.WorkflowInstanceRestartResponse;
import com.netflix.maestro.models.api.WorkflowStartResponse;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineStatusEvent;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Response of the entity (workflow instance or step instance) run request. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString(exclude = {"instance"})
public class RunResponse {
  private final String workflowId;
  private final long workflowVersionId;
  private final long workflowInstanceId;
  private final long workflowRunId;
  private final String workflowUuid;
  private final Status status;
  private final TimelineEvent timelineEvent;
  @Nullable private final String stepId;
  @Nullable private final Long stepAttemptId;

  @Nullable private final WorkflowInstance instance;

  /** RunResponse status. */
  public enum Status {
    /** workflow instance run is created. */
    WORKFLOW_RUN_CREATED(InstanceRunStatus.CREATED),
    /** step instance attempt is created. */
    STEP_ATTEMPT_CREATED(InstanceRunStatus.CREATED),
    /** workflow instance with the same uuid already exists, won't create a new one. */
    DUPLICATED(InstanceRunStatus.DUPLICATED),
    /** workflow instance is stopped due to run strategy, i.e. FIRST_ONLY or LAST_ONLY. */
    STOPPED(InstanceRunStatus.STOPPED),
    /** workflow instance is delegated to its step to apply actions on it. */
    DELEGATED(InstanceRunStatus.INTERNAL_ERROR),
    /** workflow instance is in non-terminal status and cannot apply actions on it. */
    NON_TERMINAL_ERROR(InstanceRunStatus.INTERNAL_ERROR);

    private final InstanceRunStatus runStatus;

    Status(InstanceRunStatus runStatus) {
      this.runStatus = runStatus;
    }

    /** Convert an int code returned from DAO to a RunResponse Status. */
    public static Status fromCode(int code) {
      switch (code) {
        case 1:
          return WORKFLOW_RUN_CREATED;
        case 0:
          return DUPLICATED;
        case -1:
          return STOPPED;
        default:
          throw new MaestroInternalError("Invalid status code value: %s", code);
      }
    }
  }

  /**
   * static method to create a {@link RunResponse} from a {@link WorkflowInstance} and its state.
   */
  public static RunResponse from(WorkflowInstance instance, int state) {
    return RunResponse.builder()
        .workflowId(instance.getWorkflowId())
        .workflowVersionId(instance.getWorkflowVersionId())
        .workflowInstanceId(instance.getWorkflowInstanceId())
        .workflowRunId(instance.getWorkflowRunId())
        .workflowUuid(instance.getWorkflowUuid())
        .status(Status.fromCode(state))
        .timelineEvent(instance.getInitiator().getTimelineEvent())
        .build();
  }

  /**
   * static method to create a {@link RunResponse} from a {@link WorkflowInstance} and its step id.
   */
  public static RunResponse from(WorkflowInstance instance, String stepId) {
    RunResponseBuilder builder =
        RunResponse.builder()
            .workflowId(instance.getWorkflowId())
            .workflowVersionId(instance.getWorkflowVersionId())
            .workflowInstanceId(instance.getWorkflowInstanceId())
            .workflowRunId(instance.getWorkflowRunId())
            .workflowUuid(instance.getWorkflowUuid())
            .timelineEvent(
                TimelineStatusEvent.create(instance.getModifyTime(), instance.getStatus()));
    if (stepId != null) {
      builder.status(Status.DELEGATED).stepId(stepId).instance(instance);
    } else {
      builder.status(Status.NON_TERMINAL_ERROR);
    }
    return builder.build();
  }

  /**
   * static method to create a {@link RunResponse} from a {@link StepInstance} and its timeline
   * event.
   */
  public static RunResponse from(StepInstance stepInstance, TimelineEvent event) {
    return RunResponse.builder()
        .workflowId(stepInstance.getWorkflowId())
        .workflowVersionId(stepInstance.getWorkflowVersionId())
        .workflowInstanceId(stepInstance.getWorkflowInstanceId())
        .workflowRunId(stepInstance.getWorkflowRunId())
        .workflowUuid(stepInstance.getWorkflowUuid())
        .status(Status.STEP_ATTEMPT_CREATED)
        .timelineEvent(event)
        .stepId(stepInstance.getStepId())
        .stepAttemptId(stepInstance.getStepAttemptId() + 1)
        .build();
  }

  /** transform RunResponse to WorkflowStartResponse. */
  public WorkflowStartResponse toWorkflowStartResponse() {
    return WorkflowStartResponse.builder()
        .workflowId(this.workflowId)
        .workflowVersionId(this.workflowVersionId)
        .workflowInstanceId(this.workflowInstanceId)
        .workflowRunId(this.workflowRunId)
        .workflowUuid(this.workflowUuid)
        .status(this.status.runStatus)
        .timelineEvent(this.timelineEvent)
        .build();
  }

  /** transform RunResponse to WorkflowInstanceRestartResponse. */
  public WorkflowInstanceRestartResponse toWorkflowRestartResponse() {
    return WorkflowInstanceRestartResponse.builder()
        .workflowId(this.workflowId)
        .workflowVersionId(this.workflowVersionId)
        .workflowInstanceId(this.workflowInstanceId)
        .workflowRunId(this.workflowRunId)
        .workflowUuid(this.workflowUuid)
        .status(this.status.runStatus)
        .timelineEvent(this.timelineEvent)
        .build();
  }

  /** transform RunResponse to StepInstanceRestartResponse. */
  public StepInstanceRestartResponse toStepRestartResponse() {
    return StepInstanceRestartResponse.builder()
        .workflowId(this.workflowId)
        .workflowVersionId(this.workflowVersionId)
        .workflowInstanceId(this.workflowInstanceId)
        .workflowRunId(this.workflowRunId)
        .stepId(this.stepId)
        .stepAttemptId(this.stepAttemptId)
        .status(this.status.runStatus)
        .timelineEvent(this.timelineEvent)
        .build();
  }

  /** transform RunResponse to StepInstanceActionResponse. */
  public StepInstanceActionResponse toStepInstanceActionResponse() {
    return StepInstanceActionResponse.builder()
        .workflowId(this.workflowId)
        .workflowInstanceId(this.workflowInstanceId)
        .workflowRunId(this.workflowRunId)
        .stepId(this.stepId)
        .stepAttemptId(this.stepAttemptId)
        .timelineEvent(this.timelineEvent)
        .build();
  }
}
