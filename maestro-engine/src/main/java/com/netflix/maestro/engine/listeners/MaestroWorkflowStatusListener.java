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
package com.netflix.maestro.engine.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowStatusListener;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.metrics.MaestroMetrics;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.tasks.MaestroStartTask;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.engine.utils.AggregatedViewHelper;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro workflow status listener implementation. It cleans up all the status inconsistency and
 * provides the eventual consistency at the end of the execution with at least once guarantee.
 */
@Slf4j
@AllArgsConstructor
public class MaestroWorkflowStatusListener implements WorkflowStatusListener {
  private static final String TYPE_TAG = "type";
  private static final String FAILURE_REASON_PREFIX = WorkflowInstance.Status.FAILED.name() + "-";

  private final MaestroTask maestroTask;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final MaestroJobEventPublisher publisher;
  private final ObjectMapper objectMapper;
  private final MaestroMetrics metrics;

  @Override
  public void onWorkflowCompleted(Workflow workflow) {
    LOG.trace("Workflow {} is completed", workflow.getWorkflowId());
    metrics.counter(
        MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
        getClass(),
        TYPE_TAG,
        "onWorkflowCompleted");
  }

  @Override
  public void onWorkflowTerminated(Workflow workflow) {
    LOG.trace(
        "Workflow {} is terminated with status {}", workflow.getWorkflowId(), workflow.getStatus());
    metrics.counter(
        MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
        getClass(),
        TYPE_TAG,
        "onWorkflowTerminated",
        MetricConstants.STATUS_TAG,
        workflow.getStatus().name());
  }

  @Override
  public void onWorkflowFinalized(Workflow workflow) {
    WorkflowSummary summary = StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());
    WorkflowRuntimeSummary runtimeSummary = retrieveWorkflowRuntimeSummary(workflow);
    String reason = workflow.getReasonForIncompletion();
    LOG.info(
        "Workflow {} with execution_id [{}] is finalized with internal state [{}] and reason [{}]",
        summary.getIdentity(),
        workflow.getWorkflowId(),
        workflow.getStatus(),
        reason);
    metrics.counter(
        MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
        getClass(),
        TYPE_TAG,
        "onWorkflowFinalized",
        MetricConstants.STATUS_TAG,
        workflow.getStatus().name());

    if (reason != null
        && workflow.getStatus() == Workflow.WorkflowStatus.FAILED
        && reason.startsWith(MaestroStartTask.DEDUP_FAILURE_PREFIX)) {
      LOG.info(
          "Workflow {} with execution_id [{}] has not actually started, thus skip onWorkflowFinalized.",
          summary.getIdentity(),
          workflow.getWorkflowId());
      return; // special case doing nothing
    }

    WorkflowInstance.Status instanceStatus =
        instanceDao.getWorkflowInstanceStatus(
            summary.getWorkflowId(), summary.getWorkflowInstanceId(), summary.getWorkflowRunId());
    if (instanceStatus == null
        || (instanceStatus.isTerminal() && workflow.getStatus().isTerminal())) {
      LOG.info(
          "Workflow {} with execution_id [{}] does not exist or already "
              + "in a terminal state [{}] with internal state [{}], thus skip onWorkflowFinalized.",
          summary.getIdentity(),
          workflow.getWorkflowId(),
          instanceStatus,
          workflow.getStatus());
      return;
    }

    Map<String, Task> realTaskMap = TaskHelper.getUserDefinedRealTaskMap(workflow);
    // cancel internally failed tasks
    realTaskMap.values().stream()
        .filter(task -> !StepHelper.retrieveStepStatus(task.getOutputData()).isTerminal())
        .forEach(task -> maestroTask.cancel(workflow, task, null));

    WorkflowRuntimeOverview overview =
        TaskHelper.computeOverview(
            objectMapper, summary, runtimeSummary.getRollupBase(), realTaskMap);

    try {
      validateAndUpdateOverview(overview, summary);
      switch (workflow.getStatus()) {
        case TERMINATED: // stopped due to stop request
          if (reason != null && reason.startsWith(FAILURE_REASON_PREFIX)) {
            update(workflow, WorkflowInstance.Status.FAILED, summary, overview);
          } else {
            update(workflow, WorkflowInstance.Status.STOPPED, summary, overview);
          }
          break;
        case TIMED_OUT:
          update(workflow, WorkflowInstance.Status.TIMED_OUT, summary, overview);
          break;
        default: // other status (FAILED, COMPLETED, PAUSED, RUNNING) to be handled here.
          Optional<Task.Status> done =
              TaskHelper.checkProgress(realTaskMap, summary, overview, true);
          switch (done.orElse(Task.Status.IN_PROGRESS)) {
              /**
               * This is a special status to indicate that the workflow has succeeded. Check {@link
               * TaskHelper#checkProgress} for more details.
               */
            case FAILED_WITH_TERMINAL_ERROR:
              WorkflowInstance.Status nextStatus =
                  AggregatedViewHelper.deriveAggregatedStatus(
                      instanceDao, summary, WorkflowInstance.Status.SUCCEEDED, overview);
              if (!nextStatus.isTerminal()) {
                throw new MaestroInternalError(
                    "Invalid status: [%s], expecting a terminal one", nextStatus);
              }
              update(workflow, nextStatus, summary, overview);
              break;
            case FAILED:
            case CANCELED: // due to step failure
              update(workflow, WorkflowInstance.Status.FAILED, summary, overview);
              break;
            case TIMED_OUT:
              update(workflow, WorkflowInstance.Status.TIMED_OUT, summary, overview);
              break;
              // all other status are invalid
            default:
              metrics.counter(
                  MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                  getClass(),
                  TYPE_TAG,
                  "invalidStatusOnWorkflowFinalized");
              throw new MaestroInternalError(
                  "Invalid status [%s] onWorkflowFinalized", workflow.getStatus());
          }
          break;
      }
    } catch (MaestroInternalError | IllegalArgumentException e) {
      // non-retryable error and still fail the instance
      LOG.warn("onWorkflowFinalized is failed with a non-retryable error", e);
      metrics.counter(
          MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
          getClass(),
          TYPE_TAG,
          "nonRetryableErrorOnWorkflowFinalized");
      update(
          workflow,
          WorkflowInstance.Status.FAILED,
          summary,
          overview,
          Details.create(
              e.getMessage(), "onWorkflowFinalized is failed with non-retryable error."));
    }
  }

  private void update(
      Workflow workflow,
      WorkflowInstance.Status status,
      WorkflowSummary summary,
      WorkflowRuntimeOverview overview) {
    update(workflow, status, summary, overview, null);
  }

  private void update(
      Workflow workflow,
      WorkflowInstance.Status status,
      WorkflowSummary summary,
      WorkflowRuntimeOverview overview,
      Details errorDetails) {
    long markTime = System.currentTimeMillis();
    WorkflowRuntimeSummary runtimeSummary = retrieveWorkflowRuntimeSummary(workflow);
    runtimeSummary.addTimeline(
        TimelineLogEvent.info(
            "Workflow instance status is updated to [%s] due to [%s]",
            status, workflow.getReasonForIncompletion()));
    if (errorDetails != null) {
      runtimeSummary.addTimeline(TimelineDetailsEvent.from(errorDetails));
    }
    Optional<Details> updated =
        instanceDao.updateWorkflowInstance(
            summary, overview, runtimeSummary.getTimeline(), status, markTime);
    if (updated.isPresent()) {
      LOG.error(
          "Failed when finalizing workflow {} with execution_id [{}] due to {}, Will retry.",
          summary.getIdentity(),
          workflow.getWorkflowId(),
          updated.get());
      metrics.counter(
          MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
          getClass(),
          TYPE_TAG,
          "failedUpdateOnWorkflowFinalized");
      throw new MaestroRetryableError(
          updated.get(), "Failed to update workflow instance: " + summary.getIdentity());
    }

    publisher.publishOrThrow(
        WorkflowInstanceUpdateJobEvent.create(summary, runtimeSummary, status, markTime),
        "Failed to publish maestro job event when finalizing workflow: " + summary.getIdentity());

    metrics.counter(
        MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
        getClass(),
        TYPE_TAG,
        "updateOnWorkflowFinalized",
        MetricConstants.STATUS_TAG,
        status.name());
  }

  private WorkflowRuntimeSummary retrieveWorkflowRuntimeSummary(Workflow workflow) {
    Task task = workflow.getTaskByRefName(Constants.DEFAULT_END_STEP_NAME);
    if (task == null) {
      return new WorkflowRuntimeSummary();
    }
    return StepHelper.retrieveWorkflowRuntimeSummary(objectMapper, task.getOutputData());
  }

  private void validateAndUpdateOverview(
      WorkflowRuntimeOverview overview, WorkflowSummary summary) {
    Map<String, StepRuntimeState> states =
        stepInstanceDao.getAllStepStates(
            summary.getWorkflowId(), summary.getWorkflowInstanceId(), summary.getWorkflowRunId());

    EnumMap<StepInstance.Status, WorkflowStepStatusSummary> stats =
        TaskHelper.toStepStatusMap(summary, states);

    if (!stats.keySet().stream().allMatch(StepInstance.Status::isTerminal)) {
      LOG.error(
          "Invalid status: all step instances must be in terminal state [{}] for workflow {}, will retry",
          stats,
          summary.getIdentity());
      metrics.counter(
          MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
          getClass(),
          TYPE_TAG,
          "nonTerminalStatusOnWorkflowFinalized");
      throw new MaestroRetryableError(
          "Final workflow %s step status is invalid and will retry termination.",
          summary.getIdentity());
    }

    // This can happen if the workflow instance might miss the latest updates. Will sync again.
    if (!statsMatched(stats, overview.getStepOverview(), summary)) {
      LOG.warn(
          "maestro step stats [{}] are inconsistent with internal stats [{}] for workflow {} and syncing again",
          stats,
          overview.getStepOverview(),
          summary.getIdentity());
      metrics.counter(
          MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
          getClass(),
          TYPE_TAG,
          "inconsistentStatsOnWorkflowFinalized");
    }
    overview.setStepOverview(stats);
  }

  private boolean statsMatched(
      Map<StepInstance.Status, WorkflowStepStatusSummary> stats,
      Map<StepInstance.Status, WorkflowStepStatusSummary> stepStatusMap,
      WorkflowSummary summary) {
    LOG.debug(
        "Comparing maestro step stats [{}] with internal stats [{}] for workflow {}",
        stats,
        stepStatusMap,
        summary.getIdentity());
    return stats.entrySet().stream()
        .allMatch(
            entry ->
                stepStatusMap.containsKey(entry.getKey())
                    && stepStatusMap.get(entry.getKey()).equals(entry.getValue()));
  }
}
