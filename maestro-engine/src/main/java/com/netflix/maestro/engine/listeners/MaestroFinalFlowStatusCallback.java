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
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.tasks.MaestroStartTask;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.engine.utils.AggregatedViewHelper;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.runtime.FinalFlowStatusCallback;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro final flow status callback implementation. It cleans up all the status inconsistency and
 * provides the eventual consistency at the end of the execution with at least once guarantee.
 */
@Slf4j
@AllArgsConstructor
public class MaestroFinalFlowStatusCallback implements FinalFlowStatusCallback {
  private static final String FAILURE_REASON_PREFIX = WorkflowInstance.Status.FAILED.name() + "-";

  private final MaestroTask maestroTask;
  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final ObjectMapper objectMapper;
  private final MaestroMetrics metrics;

  @Override
  public void onFlowCompleted(Flow flow) {
    LOG.trace("Flow {} is completed", flow.getFlowId());
    metrics.counter(
        MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
        getClass(),
        MetricConstants.TYPE_TAG,
        "onFlowCompleted");
  }

  @Override
  public void onFlowTerminated(Flow flow) {
    LOG.trace("Flow {} is terminated with status {}", flow.getFlowId(), flow.getStatus());
    metrics.counter(
        MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
        getClass(),
        MetricConstants.TYPE_TAG,
        "onFlowTerminated",
        MetricConstants.STATUS_TAG,
        flow.getStatus().name());
  }

  @Override
  public void onFlowFinalized(Flow flow) {
    WorkflowSummary summary = StepHelper.retrieveWorkflowSummary(objectMapper, flow.getInput());
    WorkflowRuntimeSummary runtimeSummary = retrieveWorkflowRuntimeSummary(flow);
    String reason = flow.getReasonForIncompletion();
    LOG.info(
        "Workflow {} with execution_id [{}] is finalized with internal state [{}] and reason [{}]",
        summary.getIdentity(),
        flow.getFlowId(),
        flow.getStatus(),
        reason);
    metrics.counter(
        MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
        getClass(),
        MetricConstants.TYPE_TAG,
        "onFlowFinalized",
        MetricConstants.STATUS_TAG,
        flow.getStatus().name());

    if (reason != null
        && flow.getStatus() == Flow.Status.FAILED
        && reason.startsWith(MaestroStartTask.DEDUP_FAILURE_PREFIX)) {
      LOG.info(
          "Workflow {} with execution_id [{}] has not actually started, thus skip onFlowFinalized.",
          summary.getIdentity(),
          flow.getFlowId());
      return; // special case doing nothing
    }

    WorkflowInstance.Status instanceStatus =
        instanceDao.getWorkflowInstanceStatus(
            summary.getWorkflowId(), summary.getWorkflowInstanceId(), summary.getWorkflowRunId());
    if (instanceStatus == null || (instanceStatus.isTerminal() && flow.getStatus().isTerminal())) {
      LOG.info(
          "Workflow {} with execution_id [{}] does not exist or already "
              + "in a terminal state [{}] with internal state [{}], thus skip onFlowFinalized.",
          summary.getIdentity(),
          flow.getFlowId(),
          instanceStatus,
          flow.getStatus());
      return;
    }

    LOG.info(
        "Workflow {} with execution_id [{}] is not in a terminal state [{}] "
            + "with internal state [{}], thus run onFlowFinalized.",
        summary.getIdentity(),
        flow.getFlowId(),
        instanceStatus,
        flow.getStatus());
    Map<String, Task> realTaskMap =
        TaskHelper.getUserDefinedRealTaskMap(flow.getStreamOfAllTasks());
    // cancel internally failed tasks
    realTaskMap.values().stream()
        .filter(task -> !StepHelper.retrieveStepStatus(task.getOutputData()).isTerminal())
        .forEach(task -> maestroTask.cancel(flow, task));

    WorkflowRuntimeOverview overview =
        TaskHelper.computeOverview(
            objectMapper, summary, runtimeSummary.getRollupBase(), realTaskMap);

    try {
      validateAndUpdateOverview(overview, summary);
      switch (flow.getStatus()) {
        case TERMINATED: // stopped due to stop request
          if (reason != null && reason.startsWith(FAILURE_REASON_PREFIX)) {
            update(flow, WorkflowInstance.Status.FAILED, summary, overview);
          } else {
            update(flow, WorkflowInstance.Status.STOPPED, summary, overview);
          }
          break;
        case TIMED_OUT:
          update(flow, WorkflowInstance.Status.TIMED_OUT, summary, overview);
          break;
        default: // other status (FAILED, COMPLETED, RUNNING) to be handled here.
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
              update(flow, nextStatus, summary, overview);
              break;
            case FAILED:
            case CANCELED: // due to step failure
              update(flow, WorkflowInstance.Status.FAILED, summary, overview);
              break;
            case TIMED_OUT:
              update(flow, WorkflowInstance.Status.TIMED_OUT, summary, overview);
              break;
              // all other status are invalid
            default:
              metrics.counter(
                  MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                  getClass(),
                  MetricConstants.TYPE_TAG,
                  "invalidStatusOnFlowFinalized");
              throw new MaestroInternalError(
                  "Invalid status [%s] onFlowFinalized", flow.getStatus());
          }
          break;
      }
    } catch (MaestroInternalError | IllegalArgumentException e) {
      // non-retryable error and still fail the instance
      LOG.warn("onFlowFinalized is failed with a non-retryable error", e);
      metrics.counter(
          MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          "nonRetryableErrorOnFlowFinalized");
      update(
          flow,
          WorkflowInstance.Status.FAILED,
          summary,
          overview,
          Details.create(e.getMessage(), "onFlowFinalized is failed with non-retryable error."));
    }
  }

  private void update(
      Flow flow,
      WorkflowInstance.Status status,
      WorkflowSummary summary,
      WorkflowRuntimeOverview overview) {
    update(flow, status, summary, overview, null);
  }

  private void update(
      Flow flow,
      WorkflowInstance.Status status,
      WorkflowSummary summary,
      WorkflowRuntimeOverview overview,
      Details errorDetails) {
    long markTime = System.currentTimeMillis();
    WorkflowRuntimeSummary runtimeSummary = retrieveWorkflowRuntimeSummary(flow);
    runtimeSummary.addTimeline(
        TimelineLogEvent.info(
            "Workflow instance status is updated to [%s] due to [%s]",
            status, flow.getReasonForIncompletion()));
    if (errorDetails != null) {
      runtimeSummary.addTimeline(TimelineDetailsEvent.from(errorDetails));
    }
    var jobEvent =
        WorkflowInstanceUpdateJobEvent.create(
            summary.getWorkflowId(),
            summary.getWorkflowName(),
            summary.getWorkflowInstanceId(),
            summary.getWorkflowRunId(),
            summary.getWorkflowUuid(),
            summary.getCorrelationId(),
            summary.getInitiator(),
            summary.getGroupInfo(),
            summary.getTags(),
            runtimeSummary.getInstanceStatus(),
            status,
            markTime);
    Optional<Details> updated =
        instanceDao.updateWorkflowInstance(
            summary, overview, runtimeSummary.getTimeline(), status, markTime, jobEvent);
    if (updated.isPresent()) {
      LOG.error(
          "Failed when finalizing workflow {} with execution_id [{}] due to {}, Will retry.",
          summary.getIdentity(),
          flow.getFlowId(),
          updated.get());
      metrics.counter(
          MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          "failedUpdateOnFlowFinalized");
      throw new MaestroRetryableError(
          updated.get(), "Failed to update workflow instance: " + summary.getIdentity());
    }
    metrics.counter(
        MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
        getClass(),
        MetricConstants.TYPE_TAG,
        "updateOnFlowFinalized",
        MetricConstants.STATUS_TAG,
        status.name());
  }

  private WorkflowRuntimeSummary retrieveWorkflowRuntimeSummary(Flow flow) {
    Task task = flow.getMonitorTask();
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
          MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          "nonTerminalStatusOnFlowFinalized");
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
          MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          "inconsistentStatsOnFlowFinalized");
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
