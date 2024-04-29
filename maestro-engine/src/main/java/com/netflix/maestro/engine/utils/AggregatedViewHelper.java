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
package com.netflix.maestro.engine.utils;

import com.netflix.maestro.annotations.VisibleForTesting;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Helper class for computing aggregated views for workflow instances and step instances. */
public final class AggregatedViewHelper {

  /** Map that defines correlation between StepInstance status and WorkflowInstance status. */
  private static final Map<StepInstance.Status, WorkflowInstance.Status>
      STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS;

  static {
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS = new EnumMap<>(StepInstance.Status.class);
    // IN_PROGRESS statuses
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.NOT_CREATED, WorkflowInstance.Status.CREATED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.CREATED, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.INITIALIZED, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.PAUSED, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.WAITING_FOR_SIGNALS, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.EVALUATING_PARAMS, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.WAITING_FOR_PERMITS, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.STARTING, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.RUNNING, WorkflowInstance.Status.IN_PROGRESS);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.FINISHING, WorkflowInstance.Status.IN_PROGRESS);

    // SUCCEEDED statuses
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.DISABLED, WorkflowInstance.Status.SUCCEEDED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.UNSATISFIED, WorkflowInstance.Status.SUCCEEDED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.SKIPPED, WorkflowInstance.Status.SUCCEEDED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.COMPLETED_WITH_ERROR, WorkflowInstance.Status.SUCCEEDED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.SUCCEEDED, WorkflowInstance.Status.SUCCEEDED);

    // FAILED statuses
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.INTERNALLY_FAILED, WorkflowInstance.Status.FAILED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.FATALLY_FAILED, WorkflowInstance.Status.FAILED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.USER_FAILED, WorkflowInstance.Status.FAILED);
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.PLATFORM_FAILED, WorkflowInstance.Status.FAILED);

    // STOPPED statuses
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.STOPPED, WorkflowInstance.Status.STOPPED);

    // TIMED_OUT statuses
    STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.put(
        StepInstance.Status.TIMED_OUT, WorkflowInstance.Status.TIMED_OUT);
  }

  private AggregatedViewHelper() {}

  /**
   * Computes aggregated view representation based on passed instance. Also computes aggregated
   * status.
   *
   * @param workflowInstance Aggregate from run 1 to passed run.
   * @param statusKnown flag to indicate status is known so no need to recompute it.
   * @return Aggregated representation from run 1 to passed run.
   */
  public static WorkflowInstanceAggregatedInfo computeAggregatedView(
      WorkflowInstance workflowInstance, boolean statusKnown) {
    if (workflowInstance == null) {
      // returning empty object since cannot access state of the current instance run
      return new WorkflowInstanceAggregatedInfo();
    }

    WorkflowInstanceAggregatedInfo instanceAggregated =
        computeAggregatedViewNoStatus(workflowInstance);
    if (statusKnown || workflowInstance.getAggregatedInfo() == null) {
      instanceAggregated.setWorkflowInstanceStatus(workflowInstance.getStatus());
    } else {
      computeAndSetAggregatedInstanceStatus(workflowInstance, instanceAggregated);
    }
    return instanceAggregated;
  }

  /**
   * Computes aggregated view representation based on passed instance. Does not compute aggregated
   * status.
   *
   * @param instance workflow instance to compute aggregated views for steps.
   * @return computed aggregated info
   */
  private static WorkflowInstanceAggregatedInfo computeAggregatedViewNoStatus(
      WorkflowInstance instance) {
    WorkflowInstanceAggregatedInfo currentAggregated = new WorkflowInstanceAggregatedInfo();

    WorkflowRuntimeOverview runtimeOverview = instance.getRuntimeOverview();
    WorkflowInstanceAggregatedInfo baselineAggregatedInfo = instance.getAggregatedInfo();
    Map<String, StepAggregatedView> runtimeState = Collections.emptyMap();

    // use the previous aggregated view or empty object since cannot access state of the current run
    if (runtimeOverview == null) {
      if (baselineAggregatedInfo != null) {
        currentAggregated
            .getStepAggregatedViews()
            .putAll(baselineAggregatedInfo.getStepAggregatedViews());
      }
    } else {
      runtimeState =
          convertToAggregated(
              runtimeOverview.decodeStepOverview(instance.getRuntimeDag()),
              instance.getWorkflowRunId());
      if (baselineAggregatedInfo == null) {
        // means this is a fresh run, just return the runtime state
        currentAggregated.setStepAggregatedViews(runtimeState);
      } else {
        currentAggregated
            .getStepAggregatedViews()
            .putAll(baselineAggregatedInfo.getStepAggregatedViews());
        currentAggregated.getStepAggregatedViews().putAll(runtimeState);
      }
    }

    // we are NOT computing aggregated status here
    populateAndResetWithNotCreated(
        instance, runtimeState.keySet(), currentAggregated.getStepAggregatedViews());
    return currentAggregated;
  }

  /**
   * Convert the StepRuntimeStates into StepAggregatedViews for the current instance.
   *
   * @param runtimeState step runtime states
   * @param workflowRunId workflow run id
   * @return step aggregated info based on step runtime states
   */
  private static Map<String, StepAggregatedView> convertToAggregated(
      Map<String, StepRuntimeState> runtimeState, Long workflowRunId) {
    Map<String, StepAggregatedView> aggregated = new LinkedHashMap<>();
    runtimeState.forEach(
        (key, value) ->
            aggregated.put(
                key,
                StepAggregatedView.builder()
                    .workflowRunId(workflowRunId)
                    .status(value.getStatus())
                    .startTime(value.getStartTime())
                    .endTime(value.getEndTime())
                    .build()));
    return aggregated;
  }

  /**
   * It first trims runtime DAG to remove NOT_CREATED steps reachable in the current run from the
   * aggregated step view, which can change at runtime, i.e. user restarting a failed step at
   * runtime.
   *
   * <p>Workflow definition might contain steps that didn't get a chance to show up in the
   * runtimeDag. To make sure we have all the steps, we populate the list with NOT_CREATED steps
   * from definition. It will also reset the step status to NOT_CREATED if the restart action resets
   * it.
   *
   * @param instance Workflow instance to derive trimmed runtime dag and also get step definition.
   * @param knownStepIds step ids known in the step runtimeStates, which are already included in
   *     aggregatedViewMap
   * @param aggregatedViewMap Aggregated view map we populate.
   */
  private static void populateAndResetWithNotCreated(
      WorkflowInstance instance,
      Set<String> knownStepIds,
      Map<String, StepAggregatedView> aggregatedViewMap) {

    DagHelper.computeStepIdsInRuntimeDag(instance, knownStepIds).stream()
        .filter(stepId -> !knownStepIds.contains(stepId))
        .forEach(aggregatedViewMap::remove);

    instance.getRuntimeWorkflow().getSteps().stream()
        .map(Step::getId)
        .filter(id -> !aggregatedViewMap.containsKey(id))
        .forEach(
            stepId ->
                aggregatedViewMap.put(
                    stepId,
                    StepAggregatedView.builder().status(StepInstance.Status.NOT_CREATED).build()));
  }

  /**
   * Computes workflow instance status from the aggregated view.
   *
   * @param currentInstance instance for which we calculate the aggregated view
   * @param aggregated aggregated info where we will put the status
   */
  @VisibleForTesting
  static void computeAndSetAggregatedInstanceStatus(
      WorkflowInstance currentInstance, WorkflowInstanceAggregatedInfo aggregated) {
    if (!currentInstance.getStatus().isTerminal()
        || currentInstance.isFreshRun()
        || !currentInstance.getStatus().equals(WorkflowInstance.Status.SUCCEEDED)) {
      aggregated.setWorkflowInstanceStatus(currentInstance.getStatus());
      return;
    }

    boolean succeeded = true;
    for (StepAggregatedView stepAggregated : aggregated.getStepAggregatedViews().values()) {
      WorkflowInstance.Status workflowStatus =
          STEP_INSTANCE_STATUS_TO_WORKFLOW_INSTANCE_STATUS.get(stepAggregated.getStatus());
      switch (workflowStatus) {
        case FAILED:
          // if any step is failed overall status is failed
          aggregated.setWorkflowInstanceStatus(WorkflowInstance.Status.FAILED);
          return;
        case TIMED_OUT:
          // if any are timed out, we want to keep going to search for failed steps
          aggregated.setWorkflowInstanceStatus(WorkflowInstance.Status.TIMED_OUT);
          break;
        case STOPPED:
          // prioritize timed out status before stopped
          if (aggregated.getWorkflowInstanceStatus() != WorkflowInstance.Status.TIMED_OUT) {
            aggregated.setWorkflowInstanceStatus(WorkflowInstance.Status.STOPPED);
          }
          break;
        case SUCCEEDED:
          break;
        case CREATED:
          // there are steps in NOT_CREATED STATUS and the workflow instance cannot be SUCCEEDED.
          succeeded = false;
          break;
        default:
          // should never reach here with IN_PROGRESS status;
          throw new MaestroInternalError(
              "State %s is not expected during aggregated status computation for"
                  + " workflow_id = %s ; workflow_instance_id = %s ; workflow_run_id = %s",
              workflowStatus,
              currentInstance.getWorkflowId(),
              currentInstance.getWorkflowInstanceId(),
              currentInstance.getWorkflowRunId());
      }
    }
    if (aggregated.getWorkflowInstanceStatus() == null) {
      if (succeeded) {
        aggregated.setWorkflowInstanceStatus(WorkflowInstance.Status.SUCCEEDED);
      } else {
        aggregated.setWorkflowInstanceStatus(
            currentInstance.getAggregatedInfo().getWorkflowInstanceStatus());
      }
    }
  }

  /**
   * Derive the aggregated workflow instance status. Also, if needed, set the run status in the
   * pending overview.
   */
  public static WorkflowInstance.Status deriveAggregatedStatus(
      MaestroWorkflowInstanceDao instanceDao,
      WorkflowSummary summary,
      WorkflowInstance.Status runStatus,
      WorkflowRuntimeOverview overviewToUpdate) {
    if (!summary.isFreshRun() && runStatus == WorkflowInstance.Status.SUCCEEDED) {
      WorkflowInstance instance =
          instanceDao.getWorkflowInstanceRun(
              summary.getWorkflowId(), summary.getWorkflowInstanceId(), summary.getWorkflowRunId());
      instance.setRuntimeOverview(overviewToUpdate);
      instance.setStatus(runStatus);
      WorkflowInstance.Status aggStatus =
          AggregatedViewHelper.computeAggregatedView(instance, false).getWorkflowInstanceStatus();
      if (aggStatus != runStatus) {
        overviewToUpdate.setRunStatus(runStatus);
        return aggStatus;
      }
    }
    return runStatus;
  }
}
