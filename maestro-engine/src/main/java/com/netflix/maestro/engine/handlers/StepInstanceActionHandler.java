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

import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.api.StepInstanceActionResponse;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;

/** Step instance action handler to support step instance actions. */
@AllArgsConstructor
public class StepInstanceActionHandler {

  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceActionDao actionDao;
  private final WorkflowInstanceActionHandler actionHandler;

  /**
   * Restart a step. Will create a new step attempt if the workflow instance is still not in a
   * terminal state, otherwise, create a new workflow instance run and starting from this step in
   * the runtime DAG. Only allow existing a single non-terminal step attempt at any time, which must
   * be the latest one.
   */
  public RunResponse restart(RunRequest runRequest) {
    if (!runRequest.isFreshRun()
        && runRequest.getCurrentPolicy() != RunPolicy.RESTART_FROM_SPECIFIC) {
      updateRunRequestForRestartFromInlineRoot(runRequest);
    }

    RunResponse runResponse = actionHandler.restartRecursively(runRequest);
    if (runResponse.getStatus() == RunResponse.Status.DELEGATED) {
      return restartDirectly(runResponse, runRequest);
    }
    return runResponse;
  }

  private void updateRunRequestForRestartFromInlineRoot(RunRequest runRequest) {
    WorkflowInstance instance =
        instanceDao.getWorkflowInstance(
            runRequest.getRestartWorkflowId(),
            runRequest.getRestartInstanceId(),
            Constants.LATEST_INSTANCE_RUN,
            true);

    Checks.checkTrue(
        instance.getStatus().isTerminal(),
        "Invalid restart from-inline-root request [%s]: instance [%s] is in non-terminal state [%s]",
        runRequest,
        instance.getIdentity(),
        instance.getStatus());

    String stepId = runRequest.getRestartStepId();
    StepAggregatedView view = instance.getAggregatedInfo().getStepAggregatedViews().get(stepId);
    Checks.checkTrue(
        view.getStatus().isTerminal(),
        "Invalid restart from-inline-root request [%s]: step %s[%s] is in non-terminal state [%s]",
        runRequest,
        instance.getIdentity(),
        stepId,
        view.getStatus());

    Checks.checkTrue(
        runRequest.getRestartConfig().getRestartPath().size() == 1,
        "Invalid restart from-inline-root request [%s]: restart-path size is not 1",
        runRequest);

    if (instance.getInitiator().getType() == Initiator.Type.FOREACH) {
      UpstreamInitiator.Info info =
          ((ForeachInitiator) instance.getInitiator()).getNonInlineParent();
      // overwrite the initial restart node
      runRequest
          .getRestartConfig()
          .getRestartPath()
          .set(0, new RestartConfig.RestartNode(info.getWorkflowId(), info.getInstanceId(), null));
    } else {
      runRequest
          .getRestartConfig()
          .getRestartPath()
          .set(
              0,
              new RestartConfig.RestartNode(
                  runRequest.getRestartWorkflowId(), runRequest.getRestartInstanceId(), null));
    }
  }

  /** Directly restart a step without going to its ancestors. */
  public RunResponse restartDirectly(RunResponse restartStepInfo, RunRequest runRequest) {
    return actionDao.restartDirectly(restartStepInfo, runRequest, true);
  }

  /** Bypasses the step dependencies. */
  public StepInstanceActionResponse bypassStepDependencies(
      String workflowId, long workflowInstanceId, String stepId, User user) {
    WorkflowInstance instance =
        instanceDao.getLatestWorkflowInstanceRun(workflowId, workflowInstanceId);
    return actionDao.bypassStepDependencies(instance, stepId, user);
  }

  /** Terminate a step instance, i.e. the latest workflow instance run's latest attempt. */
  public StepInstanceActionResponse terminate(
      String workflowId,
      long workflowInstanceId,
      String stepId,
      User user,
      Actions.StepInstanceAction action) {
    WorkflowInstance instance =
        instanceDao.getLatestWorkflowInstanceRun(workflowId, workflowInstanceId);
    if (instance.getStatus().isTerminal()) {
      throw new MaestroInvalidStatusException(
          "Cannot manually %s the step [%s] as the workflow instance %s is in a terminal state [%s]",
          action.name(), stepId, instance.getIdentity(), instance.getStatus());
    }
    return actionDao.terminate(instance, stepId, user, action);
  }

  public StepInstanceActionResponse skip(
      String workflowId, long workflowInstanceId, String stepId, User user, RunRequest runRequest) {
    WorkflowInstance instance =
        instanceDao.getWorkflowInstance(
            workflowId, workflowInstanceId, Constants.LATEST_INSTANCE_RUN, true);
    StepAggregatedView view =
        Checks.notNull(
            instance.getAggregatedInfo().getStepAggregatedViews().get(stepId),
            "Invalid: cannot find the step view of workflow step [%s][%s][%s]",
            workflowId,
            workflowInstanceId,
            stepId);
    if (view.getStatus() == StepInstance.Status.NOT_CREATED) {
      throw new MaestroBadRequestException(
          Collections.emptyList(),
          "Cannot skip step [%s][%s][%s] before it is created. Please try it again.",
          workflowId,
          workflowInstanceId,
          stepId);
    }

    List<Actions.StepInstanceAction> supportedActions =
        Actions.STEP_INSTANCE_STATUS_TO_ACTION_MAP.get(view.getStatus());
    if (supportedActions != null && !supportedActions.contains(Actions.StepInstanceAction.SKIP)) {
      throw new MaestroBadRequestException(
          Collections.emptyList(),
          "Cannot skip step [%s][%s][%s] because it is unsupported by the step action map %s",
          workflowId,
          workflowInstanceId,
          stepId,
          supportedActions);
    }

    if (!instance.getStatus().isTerminal() && view.getStatus().shouldWakeup()) {
      return actionDao.terminate(instance, stepId, user, Actions.StepInstanceAction.SKIP);
    }

    RunResponse runResponse = restart(runRequest);
    return runResponse.toStepInstanceActionResponse();
  }
}
