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
package com.netflix.maestro.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/** Class to hold the supported actions. */
public final class Actions {
  private Actions() {}

  /** Supported workflow actions. */
  public enum WorkflowAction {
    /** Validate the workflow definition. */
    VALIDATE,
    /** Start a new workflow instance. */
    START,
    /** Start a batch of new workflow instances. */
    START_BATCH,
    /** Stop all workflow instances for a workflow version. */
    STOP,
    /** Kill all workflow instances for a workflow version. */
    KILL,
    /** Activate a workflow version. */
    ACTIVATE,
    /** Deactivate a workflow. */
    DEACTIVATE,
    /** Action to flag all failed instances to unblock strict sequential. */
    UNBLOCK,
    /** todo support action to pause a workflow. */
    PAUSE,
    /** todo support action to resume a workflow. */
    RESUME
  }

  /** Supported workflow instance actions. */
  @Getter
  public enum WorkflowInstanceAction {
    /** Stop a workflow instance run and stop all its non-terminal steps. */
    STOP(WorkflowInstance.Status.STOPPED),
    /** Kill a workflow instance run and kill all its non-terminal steps. */
    KILL(WorkflowInstance.Status.FAILED),
    /** Restart a workflow instance. */
    RESTART(WorkflowInstance.Status.CREATED),
    /** Action to flag failed instance to unblock strict sequential. */
    UNBLOCK(WorkflowInstance.Status.IN_PROGRESS);

    @JsonIgnore private final WorkflowInstance.Status status;

    /** The status after the action. */
    WorkflowInstanceAction(WorkflowInstance.Status status) {
      this.status = status;
    }
  }

  /** Supported step actions. */
  public enum StepAction {
    /** Set breakpoint to pause at this step. */
    PAUSE,
    /** Remove breakpoint to resume the execution. */
    RESUME,
  }

  /** Supported step instance actions. */
  @Getter
  public enum StepInstanceAction {
    /** Set breakpoint to pause at this step. */
    PAUSE(StepInstance.Status.PAUSED, false),
    /** Remove breakpoint to resume the execution. */
    RESUME(StepInstance.Status.WAITING_FOR_SIGNALS, false),
    /** Bypass dependencies. */
    BYPASS_STEP_DEPENDENCIES(StepInstance.Status.EVALUATING_PARAMS, false),
    /**
     * Stop a step instance. Maestro will not stop the whole workflow instance if other steps are
     * running.
     */
    STOP(StepInstance.Status.STOPPED, true),
    /**
     * Internal action to time out a step instance. The action might from the upstream workflow or
     * step instance.
     */
    TIME_OUT(StepInstance.Status.TIMED_OUT, true),
    /**
     * Fail a step instance. Maestro will decide to fail the workflow or just this step by checking
     * the step failure mode.
     */
    KILL(StepInstance.Status.FATALLY_FAILED, true),
    /** Skip a step instance and then continue dag execution to the next step. */
    SKIP(StepInstance.Status.SKIPPED, true),
    /**
     * Restart a failed or stopped step instance. If the workflow is running, it creates a new step
     * attempt. Otherwise, it creates a new workflow run to execute this step.
     */
    RESTART(StepInstance.Status.CREATED, false);

    @JsonIgnore private final StepInstance.Status status;
    @JsonIgnore private final boolean usingUpstream;

    /** the status after the action. */
    StepInstanceAction(StepInstance.Status status, boolean usingUpstream) {
      this.status = status;
      this.usingUpstream = usingUpstream;
    }
  }

  /** workflow instance status to action mapping. */
  public static final Map<WorkflowInstance.Status, List<WorkflowInstanceAction>>
      WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP;

  /** step instance status to action mapping. */
  public static final Map<StepInstance.Status, List<StepInstanceAction>>
      STEP_INSTANCE_STATUS_TO_ACTION_MAP;

  static {
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP = new EnumMap<>(WorkflowInstance.Status.class);
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.CREATED,
        Arrays.asList(WorkflowInstanceAction.STOP, WorkflowInstanceAction.KILL));
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.IN_PROGRESS,
        Arrays.asList(WorkflowInstanceAction.STOP, WorkflowInstanceAction.KILL));
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.PAUSED,
        Arrays.asList(WorkflowInstanceAction.STOP, WorkflowInstanceAction.KILL));
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.TIMED_OUT,
        Collections.singletonList(WorkflowInstanceAction.RESTART));
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.STOPPED, Collections.singletonList(WorkflowInstanceAction.RESTART));
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.FAILED,
        Arrays.asList(WorkflowInstanceAction.RESTART, WorkflowInstanceAction.UNBLOCK));
    WORKFLOW_INSTANCE_STATUS_TO_ACTION_MAP.put(
        WorkflowInstance.Status.SUCCEEDED,
        Collections.singletonList(WorkflowInstanceAction.RESTART));

    STEP_INSTANCE_STATUS_TO_ACTION_MAP = new EnumMap<>(StepInstance.Status.class);
    // not support SKIP action for CREATED status because maestro_step_satisfied is uninitialized
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.CREATED,
        Arrays.asList(StepInstanceAction.PAUSE, StepInstanceAction.STOP, StepInstanceAction.KILL));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.INITIALIZED,
        Arrays.asList(
            StepInstanceAction.PAUSE,
            StepInstanceAction.STOP,
            StepInstanceAction.KILL,
            StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.PAUSED,
        Arrays.asList(
            StepInstanceAction.RESUME,
            StepInstanceAction.STOP,
            StepInstanceAction.KILL,
            StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.WAITING_FOR_SIGNALS,
        Arrays.asList(
            StepInstanceAction.STOP,
            StepInstanceAction.KILL,
            StepInstanceAction.SKIP,
            StepInstanceAction.BYPASS_STEP_DEPENDENCIES));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.EVALUATING_PARAMS,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.WAITING_FOR_PERMITS,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.STARTING,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.RUNNING,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.FINISHING,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));

    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.DISABLED, Collections.singletonList(StepInstanceAction.RESTART));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.UNSATISFIED, Collections.singletonList(StepInstanceAction.RESTART));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.SKIPPED,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.SUCCEEDED,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.COMPLETED_WITH_ERROR,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));

    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.USER_FAILED,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.PLATFORM_FAILED,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.TIMEOUT_FAILED,
        Arrays.asList(StepInstanceAction.STOP, StepInstanceAction.KILL, StepInstanceAction.SKIP));

    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.FATALLY_FAILED,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.INTERNALLY_FAILED,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.STOPPED,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));
    STEP_INSTANCE_STATUS_TO_ACTION_MAP.put(
        StepInstance.Status.TIMED_OUT,
        Arrays.asList(StepInstanceAction.RESTART, StepInstanceAction.SKIP));
  }
}
