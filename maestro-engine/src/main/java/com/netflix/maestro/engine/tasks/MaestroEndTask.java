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
package com.netflix.maestro.engine.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.utils.AggregatedViewHelper;
import com.netflix.maestro.engine.utils.RollupAggregationHelper;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.runtime.FlowTask;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.jobevents.WorkflowInstanceUpdateJobEvent;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro end task is a special gate step with three features.
 *
 * <p>This task includes:
 *
 * <p>1. make the execution following the order defined in the DAG of the Maestro workflow
 * definition.
 *
 * <p>2. update maestro workflow instance data and workflow runtime overview.
 *
 * <p>3. Additionally, it handles the at-least once workflow status change notification.
 *
 * <p>If this is the root workflow instance, it also monitors the size of the whole DAG tree. If the
 * total number of leaf steps is greater than {@link Constants#TOTAL_LEAF_STEP_COUNT_LIMIT}, it will
 * terminate this workflow instance DAG tree.
 */
@Slf4j
public final class MaestroEndTask implements FlowTask {
  private static final long WORKFLOW_LONG_START_DELAY_INTERVAL = 180000;
  private static final User END_TASK_USER = User.create(Constants.DEFAULT_END_TASK_NAME);

  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceActionDao actionDao;
  private final ObjectMapper objectMapper;
  private final RollupAggregationHelper rollupAggregationHelper;
  private final MaestroMetrics metrics;

  /** Constructor. */
  public MaestroEndTask(
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceActionDao actionDao,
      ObjectMapper objectMapper,
      RollupAggregationHelper rollupAggregationHelper,
      MaestroMetrics metricRepo) {
    this.instanceDao = instanceDao;
    this.actionDao = actionDao;
    this.objectMapper = objectMapper;
    this.rollupAggregationHelper = rollupAggregationHelper;
    this.metrics = metricRepo;
  }

  @Override
  public boolean execute(Flow flow, Task task) {
    try {
      return endJoinExecute(flow, task);
    } catch (MaestroInternalError | MaestroNotFoundException e) {
      // if an end task failed, it is a fatal error, no retry
      task.setStatus(Task.Status.FAILED_WITH_TERMINAL_ERROR);
      task.setReasonForIncompletion(e.getMessage());
      LOG.error(
          "Error executing Maestro end task: {} in flow: {}",
          task.getTaskId(),
          flow.getFlowId(),
          e);
      return true;
    }
    // Don't catch unexpected exception and the flow engine will handle it.
  }

  /**
   * End step is responsible for workflow instance update with at least once guarantee, including
   * saving maestro instance and send workflow instance status notifications.
   *
   * <p>In the end task output data, it holds workflow runtime summary instead of the step runtime
   * summary
   */
  private boolean endJoinExecute(Flow flow, Task task) {
    WorkflowSummary summary = StepHelper.retrieveWorkflowSummary(objectMapper, flow.getInput());
    WorkflowRuntimeSummary runtimeSummary =
        StepHelper.retrieveWorkflowRuntimeSummary(objectMapper, task.getOutputData());
    Map<String, Task> realTaskMap =
        TaskHelper.getUserDefinedRealTaskMap(flow.getStreamOfAllTasks());
    WorkflowRuntimeOverview newOverview =
        TaskHelper.computeOverview(
            objectMapper, summary, runtimeSummary.getRollupBase(), realTaskMap);

    Optional<Boolean> marked =
        markMaestroWorkflowStartedIfNeeded(flow, summary, runtimeSummary, newOverview);
    boolean changed = marked.isPresent();

    if (marked.orElse(true)) {
      Optional<Task.Status> done =
          TaskHelper.checkProgress(realTaskMap, summary, newOverview, false);

      if (done.isPresent()) {
        boolean updated = markMaestroWorkflowDone(summary, runtimeSummary, newOverview, done.get());
        if (updated) {
          task.setStatus(done.get()); // mutate task status
        }
        changed = true; // runtime summary is changed, persist it
      } else {
        Optional<Details> result = checkLeafStepCount(summary, newOverview);
        result.ifPresent(details -> runtimeSummary.addTimeline(TimelineDetailsEvent.from(details)));

        if (!WorkflowRuntimeOverview.isSame(runtimeSummary.getRuntimeOverview(), newOverview)) {
          updateRuntimeOverview(summary, runtimeSummary, newOverview);
          changed = true; // runtime summary is changed, persist it
        }
      }
    }
    task.getOutputData().put(Constants.WORKFLOW_RUNTIME_SUMMARY_FIELD, runtimeSummary);
    return changed;
  }

  private Optional<Details> checkLeafStepCount(
      WorkflowSummary summary, WorkflowRuntimeOverview newOverview) {
    if (summary.getInitiator().getDepth() == 0
        && newOverview.getRollupOverview() != null
        && newOverview.getRollupOverview().getTotalLeafCount()
            > Constants.TOTAL_LEAF_STEP_COUNT_LIMIT) {
      String workflowIdentity = summary.getIdentity();
      WorkflowInstance toTerminate =
          StepHelper.buildTerminateWorkflowInstance(summary, newOverview);
      String reason =
          String.format(
              "Stop instance [%s] DAG tree as its total number [%s] of leaf steps is more than system limit [%s]",
              workflowIdentity,
              newOverview.getRollupOverview().getTotalLeafCount(),
              Constants.TOTAL_LEAF_STEP_COUNT_LIMIT);
      try {
        actionDao.terminate(
            toTerminate, END_TASK_USER, Actions.WorkflowInstanceAction.STOP, reason, true);
        return Optional.of(Details.create(reason));
      } catch (RuntimeException e) {
        LOG.warn("Failed to terminate workflow [{}] and will check again", workflowIdentity);
        return Optional.of(
            Details.create(e, true, "Failed to terminate workflow and will check again"));
      }
    }
    return Optional.empty();
  }

  private void emitWorkflowDelayMetricWithTimeline(
      WorkflowRuntimeSummary runtimeSummary, WorkflowSummary workflowSummary, long dequeueTime) {
    // use the current timestamp for metric, so we can catch the delay because of retrying it.
    long workflowStartDelay = System.currentTimeMillis() - dequeueTime;
    metrics.timer(MetricConstants.WORKFLOW_START_DELAY_METRIC, workflowStartDelay, getClass());
    if (workflowStartDelay >= WORKFLOW_LONG_START_DELAY_INTERVAL) {
      LOG.info(
          "workflow [{}]  has a long start delay, took {} ms",
          workflowSummary.getIdentity(),
          workflowStartDelay);
    }

    runtimeSummary.addTimeline(
        TimelineLogEvent.builder()
            .level(TimelineEvent.Level.INFO)
            .message("Workflow instance is dequeued.")
            .timestamp(dequeueTime)
            .build());
  }

  private Optional<Boolean> markMaestroWorkflowStartedIfNeeded(
      Flow flow,
      WorkflowSummary summary,
      WorkflowRuntimeSummary runtimeSummary,
      WorkflowRuntimeOverview newOverview) {
    if (WorkflowInstance.Status.CREATED.equals(runtimeSummary.getInstanceStatus())) {
      long startTime = flow.getPrepareTask().getStartTime();
      WorkflowInstance.Status nextStatus = WorkflowInstance.Status.IN_PROGRESS;

      WorkflowInstance workflowInstance =
          instanceDao.getWorkflowInstanceRun(
              summary.getWorkflowId(), summary.getWorkflowInstanceId(), summary.getWorkflowRunId());

      runtimeSummary.setRollupBase(rollupAggregationHelper.calculateRollupBase(workflowInstance));

      emitWorkflowDelayMetricWithTimeline(runtimeSummary, summary, getDequeueTime(flow));

      return Optional.of(
          updateMaestroWorkflowInstance(
              summary, runtimeSummary, newOverview, nextStatus, startTime));
    }
    return Optional.empty();
  }

  private long getDequeueTime(Flow flow) {
    // Flow start time is the dequeue time
    if (flow.getStartTime() > 0) {
      return flow.getStartTime();
    }
    return System.currentTimeMillis();
  }

  private boolean markMaestroWorkflowDone(
      WorkflowSummary summary,
      WorkflowRuntimeSummary runtimeSummary,
      WorkflowRuntimeOverview newOverview,
      Task.Status taskStatus) {
    WorkflowInstance.Status nextStatus = WorkflowInstance.Status.SUCCEEDED;
    if (taskStatus == Task.Status.FAILED) {
      nextStatus = WorkflowInstance.Status.FAILED;
    } else if (taskStatus == Task.Status.TIMED_OUT) {
      nextStatus = WorkflowInstance.Status.TIMED_OUT;
    } else if (taskStatus == Task.Status.CANCELED) {
      nextStatus = WorkflowInstance.Status.STOPPED;
    }

    // derive the aggregated status, if different, then set it in newOverview
    nextStatus =
        AggregatedViewHelper.deriveAggregatedStatus(instanceDao, summary, nextStatus, newOverview);

    long endTime = System.currentTimeMillis(); // use the current time as the end time

    metrics.counter(
        MetricConstants.WORKFLOW_DONE_METRIC,
        getClass(),
        MetricConstants.STATUS_TAG,
        nextStatus.name(),
        MetricConstants.INITIATOR_DEPTH_TAG,
        String.valueOf(summary.getInitiator().getDepth()));

    return updateMaestroWorkflowInstance(summary, runtimeSummary, newOverview, nextStatus, endTime);
  }

  private boolean updateMaestroWorkflowInstance(
      WorkflowSummary workflowSummary,
      WorkflowRuntimeSummary runtimeSummary,
      WorkflowRuntimeOverview newOverview,
      WorkflowInstance.Status nextStatus,
      long markTime) {
    var jobEvent =
        WorkflowInstanceUpdateJobEvent.create(
            workflowSummary.getWorkflowId(),
            workflowSummary.getWorkflowName(),
            workflowSummary.getWorkflowInstanceId(),
            workflowSummary.getWorkflowRunId(),
            workflowSummary.getWorkflowUuid(),
            workflowSummary.getCorrelationId(),
            workflowSummary.getInitiator(),
            workflowSummary.getGroupInfo(),
            workflowSummary.getTags(),
            runtimeSummary.getInstanceStatus(),
            nextStatus,
            markTime);
    Optional<Details> updated =
        instanceDao.updateWorkflowInstance(
            workflowSummary,
            newOverview,
            runtimeSummary.getTimeline(),
            nextStatus,
            markTime,
            jobEvent);
    if (updated.isPresent()) {
      runtimeSummary.addTimeline(TimelineDetailsEvent.from(updated.get()));
      return false;
    }
    runtimeSummary.updateRuntimeState(nextStatus, newOverview, markTime);
    return true;
  }

  private boolean updateRuntimeOverview(
      WorkflowSummary workflowSummary,
      WorkflowRuntimeSummary runtimeSummary,
      WorkflowRuntimeOverview newOverview) {
    Optional<Details> updated =
        instanceDao.updateRuntimeOverview(
            workflowSummary, newOverview, runtimeSummary.getTimeline());
    if (updated.isPresent()) {
      runtimeSummary.addTimeline(TimelineDetailsEvent.from(updated.get()));
      return false;
    }
    runtimeSummary.setRuntimeOverview(newOverview);
    return true;
  }
}
