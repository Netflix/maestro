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
package com.netflix.maestro.engine.handlers;

import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.WorkflowInstanceActionResponse;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.TimelineActionEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Workflow action handler for a given workflow instance. */
@Slf4j
@AllArgsConstructor
public class WorkflowInstanceActionHandler {
  private final MaestroWorkflowDao workflowDao;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroRunStrategyDao runStrategyDao;
  private final MaestroStepInstanceActionDao actionDao;
  private final WorkflowHelper workflowHelper;

  /**
   * Stop the given non-terminal workflow instance's latest in-progress run. It is possible to stop
   * a given instance multiple times due to the processing delay. The stop call is idempotent and
   * should be fine. It might be asynchronous if the internal flow is running it as the flow
   * termination is asynchronous.
   *
   * <p>Note that re-run can only happen if all instance runs are in terminal states to avoid any
   * race condition.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id.
   * @return a workflow instance action response
   */
  public WorkflowInstanceActionResponse stopLatest(
      String workflowId, long workflowInstanceId, User caller) {
    return terminate(
        workflowId,
        workflowInstanceId,
        Constants.LATEST_ONE,
        Actions.WorkflowInstanceAction.STOP,
        caller);
  }

  /**
   * Kill/fail the given non-terminal workflow instance's latest in-progress run. It is possible to
   * kill a given instance multiple times due to the processing delay. The kill call is idempotent
   * and should be fine. It might be asynchronous if the internal flow is running it as the flow
   * termination is asynchronous.
   *
   * <p>Note that re-run can only happen if all instance runs are in terminal states to avoid any
   * race condition.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id.
   * @return a workflow instance action response
   */
  public WorkflowInstanceActionResponse killLatest(
      String workflowId, long workflowInstanceId, User caller) {
    return terminate(
        workflowId,
        workflowInstanceId,
        Constants.LATEST_ONE,
        Actions.WorkflowInstanceAction.KILL,
        caller);
  }

  /**
   * Stop the given non-terminal workflow instance run. It is possible to stop a given instance
   * multiple times due to the processing delay. The stop call is idempotent and should be fine. It
   * might be asynchronous if the internal flow is running it as the flow termination is
   * asynchronous.
   *
   * <p>Note that re-run can only happen if all instance runs are in terminal states to avoid any
   * race condition.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id.
   * @param workflowRunId workflow instance run id.
   * @return a workflow instance action response
   */
  public WorkflowInstanceActionResponse stop(
      String workflowId, long workflowInstanceId, long workflowRunId, User caller) {
    return terminate(
        workflowId, workflowInstanceId, workflowRunId, Actions.WorkflowInstanceAction.STOP, caller);
  }

  /**
   * Kill/fail the given non-terminal workflow instance run. It is possible to kill a given instance
   * multiple times due to the processing delay. The kill call is idempotent and should be fine. It
   * might be asynchronous if the internal flow is running it as the flow termination is
   * asynchronous.
   *
   * <p>Note that re-run can only happen if all instance runs are in terminal states to avoid any
   * race condition.
   *
   * @param workflowId workflow id
   * @param workflowInstanceId workflow instance id.
   * @param workflowRunId workflow instance run id.*
   * @return a workflow instance action response
   */
  public WorkflowInstanceActionResponse kill(
      String workflowId, long workflowInstanceId, long workflowRunId, User caller) {
    return terminate(
        workflowId, workflowInstanceId, workflowRunId, Actions.WorkflowInstanceAction.KILL, caller);
  }

  /** Return true if done, false if async. */
  private WorkflowInstanceActionResponse terminate(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      Actions.WorkflowInstanceAction action,
      User caller) {
    String reason =
        String.format(
            "User [%s] take an action to %s the running workflow instance run",
            caller.getName(), action.name());

    LOG.debug(
        "Terminating workflow instance [{}][{}][{}] to status [{}] and reason [{}]",
        workflowId,
        workflowInstanceId,
        workflowRunId,
        action.getStatus(),
        reason);

    WorkflowInstance instance =
        loadInstanceForAction(workflowId, workflowInstanceId, workflowRunId, action);

    if (instance.getExecutionId() == null
        && (instance.getStatus() == WorkflowInstance.Status.CREATED
            || instance.getStatus() == WorkflowInstance.Status.PAUSED)) {
      boolean terminated =
          instanceDao.tryTerminateQueuedInstance(instance, action.getStatus(), reason);
      if (terminated) {
        LOG.debug(
            "Marked a queued workflow instance {} with status [{}] and reason [{}]",
            instance.getIdentity(),
            action.getStatus(),
            reason);
        return WorkflowInstanceActionResponse.from(
            instance, action.getStatus(), TimelineLogEvent.info(reason), true);
      } else {
        instance =
            instanceDao.getWorkflowInstanceRun(
                workflowId, workflowInstanceId, instance.getWorkflowRunId());
      }
    }

    if (instance.getRunStatus() == action.getStatus()) {
      LOG.debug(
          "Noop termination as the instance run [{}]'s status is already reached [{}]",
          instance.getIdentity(),
          action.getStatus());
      return WorkflowInstanceActionResponse.from(
          instance, action.getStatus(), TimelineLogEvent.info(reason), true);
    }

    if (instance.getStatus().isTerminal()) {
      throw new MaestroInvalidStatusException(
          "Cannot terminate a workflow instance %s from status [%s] to [%s]",
          instance.getIdentity(), instance.getStatus(), action.getStatus());
    }

    // this is asynchronous as flow termination is asynchronous.
    Checks.notNull(
        instance.getExecutionId(),
        "workflow instance %s execution_id cannot be null",
        instance.getIdentity());

    actionDao.terminate(instance, caller, action, "manual workflow instance API call");

    return WorkflowInstanceActionResponse.from(
        instance, action.getStatus(), TimelineLogEvent.info(reason), false);
  }

  private WorkflowInstance loadInstanceForAction(
      String workflowId,
      long workflowInstanceId,
      long workflowRunId,
      Actions.WorkflowInstanceAction action) {
    WorkflowInstance instance =
        instanceDao.getLatestWorkflowInstanceRun(workflowId, workflowInstanceId);
    if (workflowRunId != Constants.LATEST_ONE && workflowRunId != instance.getWorkflowRunId()) {
      throw new MaestroBadRequestException(
          Collections.emptyList(),
          "Cannot %s the workflow instance run [%s] as it is not the latest run [%s]",
          action.name(),
          workflowRunId,
          instance.getWorkflowRunId());
    }
    return instance;
  }

  /**
   * Unblock the latest failed workflow instance run for strict sequential run strategy. It
   * internally marks the status from FAILED as FAILED_1.
   */
  public WorkflowInstanceActionResponse unblockLatest(
      String workflowId, long workflowInstanceId, User caller) {
    return unblock(workflowId, workflowInstanceId, Constants.LATEST_ONE, caller);
  }

  /**
   * Unblock a failed workflow instance run for strict sequential run strategy. It internally marks
   * the status from FAILED as FAILED_1. This action can only apply to the latest workflow instance
   * run.
   */
  public WorkflowInstanceActionResponse unblock(
      String workflowId, long workflowInstanceId, long workflowRunId, User caller) {
    WorkflowInstance instance =
        loadInstanceForAction(
            workflowId, workflowInstanceId, workflowRunId, Actions.WorkflowInstanceAction.UNBLOCK);
    LOG.debug("Unblocking workflow instance {} by user [{}]", instance.getIdentity(), caller);

    if (instance.getStatus() != WorkflowInstance.Status.FAILED) {
      throw new MaestroInvalidStatusException(
          "Cannot unblock a workflow instance %s from status [%s] as it is not FAILED status",
          instance.getIdentity(), instance.getStatus());
    }

    TimelineEvent event =
        TimelineActionEvent.builder()
            .action(Actions.WorkflowAction.UNBLOCK)
            .author(caller)
            .reason("[API] call to UNBLOCK a failed workflow instance run.")
            .build();
    boolean updated =
        instanceDao.tryUnblockFailedWorkflowInstance(
            workflowId, workflowInstanceId, instance.getWorkflowRunId(), event);
    workflowHelper.publishStartWorkflowEvent(workflowId, updated);
    return WorkflowInstanceActionResponse.from(instance, event, updated);
  }

  /**
   * Restart a workflow instance run asynchronous from the latest run. It first created an updated
   * workflow instance run in DB and then put it in a job queue. After run strategy check, the new
   * workflow instance run then gets launched. It won't reuse the existing run as maestro won't
   * overwrite the existing instance in terminal state. Only allow existing a single non-terminal
   * workflow instance run at any time, which must be the latest one.
   *
   * <p>For upstream initiated workflow runs, restart will trace up to the closest ancestor in
   * non-terminal status or the root ancestor. Then the ancestor instance or its step will restart
   * the whole chain. If it is a root but still in non-terminal status, an exception will be thrown.
   *
   * @param runRequest the prepared run request
   * @return the run response
   */
  public RunResponse restart(RunRequest runRequest) {
    RunResponse runResponse = restartRecursively(runRequest);
    if (runResponse.getStatus() == RunResponse.Status.NON_TERMINAL_ERROR) {
      LOG.error(
          "workflow instance {} does not support restart action as it is in a non-terminal status [{}]",
          runRequest.getWorkflowIdentity(),
          runResponse.getTimelineEvent().getMessage());
      throw new MaestroBadRequestException(
          Collections.emptyList(),
          "workflow instance %s does not support restart action as it is in a non-terminal status [%s]",
          runRequest.getWorkflowIdentity(),
          runResponse.getTimelineEvent().getMessage());
    }
    return runResponse;
  }

  /**
   * Recursively restart a workflow instance by reversely tracing up along the ancestor chain. If
   * there is a still-running workflow instance along the ancestor chain, it returns the info back
   * to the caller without doing any action. If all instances are in terminal states, it applies
   * changes from the run request and updates the terminated workflow instance. It then persists
   * updated workflow instance run in DB and then put it in a job queue. after run strategy check,
   * the new workflow instance run then gets launched. Only allow existing a single non-terminal
   * workflow instance in non-terminal state at any time, which must be the latest one.
   *
   * @param runRequest the run request including some changes from restart request
   * @return the run response
   */
  RunResponse restartRecursively(RunRequest runRequest) {
    WorkflowInstance instance =
        instanceDao.getWorkflowInstance(
            runRequest.getRestartWorkflowId(),
            runRequest.getRestartInstanceId(),
            Constants.LATEST_INSTANCE_RUN,
            true);

    Optional.ofNullable(runRequest.getRestartConfig())
        .flatMap(config -> Optional.ofNullable(RunRequest.getCurrentNode(config).getStepId()))
        .ifPresent(
            stepId -> {
              StepAggregatedView view =
                  instance.getAggregatedInfo().getStepAggregatedViews().get(stepId);
              Checks.checkTrue(
                  view.getStatus() != StepInstance.Status.NOT_CREATED,
                  "Invalid restart request [%s]: step [%s] is not created in the DAG.",
                  runRequest,
                  stepId);
            });

    if (instance.getStatus().isTerminal()) {
      if (instance.getInitiator() instanceof UpstreamInitiator) {
        UpstreamInitiator.Info info = ((UpstreamInitiator) instance.getInitiator()).getParent();
        runRequest.updateForUpstream(info.getWorkflowId(), info.getInstanceId(), info.getStepId());
        return restartRecursively(runRequest);
      } else {
        return restartDirectly(instance, runRequest);
      }
    } else {
      return RunResponse.from(instance, runRequest.getRestartStepId());
    }
  }

  /**
   * Directly restart an existing workflow instance without recursively tracing up and apply the
   * changes from the run request. Only allow a single non-terminal workflow instance run at any
   * time, which must be the latest one.
   *
   * @param instance workflow instance to be restarted with out-dated info, e.g. run_id
   * @param runRequest run request to restart the workflow instance
   * @return the run response
   */
  public RunResponse restartDirectly(WorkflowInstance instance, RunRequest runRequest) {
    Checks.checkTrue(
        !runRequest.isFreshRun(),
        "Cannot restart a workflow instance %s using fresh run policy [%s]",
        instance.getIdentity(),
        runRequest.getCurrentPolicy());
    if (runRequest.getRestartConfig() != null) {
      runRequest.validateIdentity(instance);
    }

    WorkflowInstance.Status status =
        instanceDao.getLatestWorkflowInstanceStatus(
            instance.getWorkflowId(), instance.getWorkflowInstanceId());
    if (!status.isTerminal()) {
      throw new MaestroInvalidStatusException(
          "Cannot restart workflow instance [%s][%s] as the latest run status is [%s] (non-terminal).",
          instance.getWorkflowId(), instance.getWorkflowInstanceId(), status);
    }

    workflowHelper.updateWorkflowInstance(instance, runRequest);
    int ret =
        runStrategyDao.startWithRunStrategy(
            instance, workflowDao.getRunStrategy(instance.getWorkflowId()));
    RunResponse runResponse = RunResponse.from(instance, ret);
    LOG.info("Restart a workflow instance with a response: [{}]", runResponse);
    return runResponse;
  }
}
