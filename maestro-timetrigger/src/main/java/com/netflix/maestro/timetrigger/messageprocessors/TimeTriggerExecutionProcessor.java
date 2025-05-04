/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.timetrigger.messageprocessors;

import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.utils.ExceptionClassifier;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.timetrigger.Constants;
import com.netflix.maestro.timetrigger.metrics.MetricConstants;
import com.netflix.maestro.timetrigger.models.PlannedTimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import com.netflix.maestro.timetrigger.properties.TimeTriggerProperties;
import com.netflix.maestro.timetrigger.utils.MaestroWorkflowLauncher;
import com.netflix.maestro.timetrigger.utils.TimeTriggerExecutionPlanner;
import com.netflix.maestro.utils.Checks;
import com.netflix.maestro.utils.ObjectHelper;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Processor class to process execution messages for time triggers. */
@Slf4j
@AllArgsConstructor
public class TimeTriggerExecutionProcessor {
  private TimeTriggerProducer timeTriggerProducer;
  private MaestroWorkflowLauncher maestroWorkflowLauncher;
  private TimeTriggerExecutionPlanner executionPlanner;
  private TimeTriggerProperties props;
  private MaestroMetrics metrics;

  public void process(Supplier<TimeTriggerExecution> messageSupplier) {
    long start = System.currentTimeMillis();
    metrics.counter(MetricConstants.EXECUTION_PROCESS_METRIC, getClass());

    TimeTriggerExecution timeTriggerExecution = messageSupplier.get();
    String workflowId = timeTriggerExecution.getWorkflowId();
    Checks.checkTrue(
        !timeTriggerExecution.getTimeTriggersWithWatermarks().isEmpty(),
        "Time triggers must not be empty: " + timeTriggerExecution);

    // Compute next executions
    List<PlannedTimeTriggerExecution> plannedTimeTriggerExecutions =
        executionPlanner.calculatePlannedExecutions(
            timeTriggerExecution.getTimeTriggersWithWatermarks(), new Date(), workflowId);

    if (!plannedTimeTriggerExecutions.isEmpty()) {
      LOG.info("Found target executions to be triggered=[{}]", plannedTimeTriggerExecutions);
    }
    // trigger a batch of executions
    List<PlannedTimeTriggerExecution> executedTimeTriggers = new ArrayList<>();
    List<PlannedTimeTriggerExecution> executionBatch =
        plannedTimeTriggerExecutions.subList(
            0, Math.min(props.getMaxTriggersPerMessage(), plannedTimeTriggerExecutions.size()));
    List<List<PlannedTimeTriggerExecution>> partitionedBatch =
        ObjectHelper.partitionList(executionBatch, props.getTriggerBatchSize());

    for (List<PlannedTimeTriggerExecution> batch : partitionedBatch) {
      boolean terminateCondition = false;
      try {
        LOG.info("Triggering batch of [{}] executions", batch.size());
        List<PlannedTimeTriggerExecution> triggeredExecutions =
            triggerPlannedExecutions(timeTriggerExecution, batch);
        executedTimeTriggers.addAll(triggeredExecutions);
        metrics.counter(
            MetricConstants.EXECUTION_TRIGGER_WORKFLOW_METRIC,
            getClass(),
            MetricConstants.TYPE_TAG,
            batch.size() > 1 ? MetricConstants.TAG_VALUE_BATCH : MetricConstants.TAG_VALUE_SINGLE);
      } catch (MaestroNotFoundException e) {
        terminateCondition = handleMaestroNotFoundException(e, timeTriggerExecution);
      } catch (MaestroResourceConflictException e) {
        metrics.counter(
            MetricConstants.EXECUTION_ERROR_METRIC,
            getClass(),
            MetricConstants.TYPE_TAG,
            MetricConstants.TAG_VALUE_CONFLICT);
        LOG.info(
            "Trigger UUID Expired for workflow [{}][{}] triggerUUID=[{}]",
            timeTriggerExecution.getWorkflowId(),
            timeTriggerExecution.getWorkflowVersion(),
            timeTriggerExecution.getWorkflowTriggerUuid(),
            e);
        terminateCondition = true;
      } catch (MaestroUnprocessableEntityException e) {
        terminateCondition = handleMaestroUnprocessableEntityException(e, timeTriggerExecution);
      }
      if (terminateCondition) {
        metrics.counter(MetricConstants.TERMINATE_SUBSCRIPTION_METRIC, getClass());
        LOG.info(
            "TimeTrigger Subscription terminating for [{}][{}][{}]",
            timeTriggerExecution.getWorkflowId(),
            timeTriggerExecution.getWorkflowVersion(),
            timeTriggerExecution.getWorkflowTriggerUuid());
        return;
      }
    }

    // calculate next execution from previously executed batches
    TimeTriggerExecution nextExecution =
        executionPlanner.constructNextExecution(timeTriggerExecution, executedTimeTriggers);

    // Calculate earliest next execution for invisibility duration
    Optional<Date> firstExecutionDate =
        executionPlanner.calculateEarliestExecutionDate(
            nextExecution.getTimeTriggersWithWatermarks(), workflowId);
    if (firstExecutionDate.isPresent()) {
      long duration = calculateMessageDelay(firstExecutionDate.get());
      LOG.info(
          "Pushing next execution with invisibility duration of [{}] seconds for [{}] with next execution date=[{}]",
          duration,
          nextExecution,
          firstExecutionDate);
      timeTriggerProducer.push(nextExecution, (int) duration);
    } else {
      // No next executions left, can terminate subscription
      metrics.counter(MetricConstants.TERMINATE_SUBSCRIPTION_METRIC, getClass());
      LOG.info(
          "timeTriggerExecution terminating for workflow [{}][{}] triggerUUID=[{}] due to empty next execution",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid());
    }
    LOG.info(
        "Finished processing timeTriggerExecution [{}] and spent [{}] ms",
        timeTriggerExecution,
        System.currentTimeMillis() - start);
  }

  private long calculateMessageDelay(Date firstExecutionDate) {
    long fullDelayForExecution =
        new BigDecimal(firstExecutionDate.getTime() - System.currentTimeMillis())
            .divide(new BigDecimal(Constants.MILLISECONDS_CONVERT), RoundingMode.CEILING)
            .longValue();
    LOG.info(
        "The delay interval is [{}] before the next execution date [{}].",
        fullDelayForExecution,
        firstExecutionDate);
    if (fullDelayForExecution > props.getMaxDelay()) {
      fullDelayForExecution =
          props.getMaxDelay() - props.getMaxJitter() + new Random().nextInt(props.getMaxJitter());
    }
    return Math.max(0, fullDelayForExecution);
  }

  private List<PlannedTimeTriggerExecution> triggerPlannedExecutions(
      TimeTriggerExecution execution, List<PlannedTimeTriggerExecution> executionBatch) {

    Checks.checkTrue(!executionBatch.isEmpty(), "Execution batch should be > 0");

    LOG.info(
        "Triggering execution for workflow [{}][{}] triggerUUID=[{}] with planned execution dates [{}]",
        execution.getWorkflowId(),
        execution.getWorkflowVersion(),
        execution.getWorkflowTriggerUuid(),
        executionBatch.stream().map(PlannedTimeTriggerExecution::executionDate).toList());

    List<RunResponse> workflowStartResponse =
        maestroWorkflowLauncher.startWorkflowBatchRuns(
            execution.getWorkflowId(),
            execution.getWorkflowVersion(),
            execution.getWorkflowTriggerUuid(),
            executionBatch);
    LOG.info("Workflow Launch Response is [{}]", workflowStartResponse);
    Checks.checkTrue(
        workflowStartResponse.size() == executionBatch.size(),
        "WorkflowStartResponse list must be same size as submitted");
    return executionBatch;
  }

  private boolean handleMaestroNotFoundException(
      MaestroNotFoundException e, TimeTriggerExecution timeTriggerExecution) {
    if (ExceptionClassifier.isWorkflowNotFoundException(e)) {
      metrics.counter(
          MetricConstants.EXECUTION_ERROR_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          MetricConstants.TAG_VALUE_NOT_FOUND);
      LOG.info(
          "Workflow not found for workflow [{}][{}] triggerUUID=[{}]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid());
      return true;
    } else if (ExceptionClassifier.isNoActiveWorkflowVersionException(e)) {
      metrics.counter(
          MetricConstants.EXECUTION_ERROR_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          MetricConstants.TAG_VALUE_INACTIVE);
      LOG.info(
          "Workflow not active for workflow [{}][{}] triggerUUID=[{}]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid());
      return true;
    } else {
      metrics.counter(
          MetricConstants.EXECUTION_ERROR_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          MetricConstants.TAG_VALUE_UNKNOWN_404);
      LOG.error(
          "Unknown 404 not found error for workflow [{}][{}] triggerUUID=[{}]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid(),
          e);
      throw new MaestroRetryableError(
          e,
          "Unknown 404 not found error for workflow [%s][%s] triggerUUID=[%s]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid());
    }
  }

  private boolean handleMaestroUnprocessableEntityException(
      MaestroUnprocessableEntityException e, TimeTriggerExecution timeTriggerExecution) {
    if (ExceptionClassifier.isTimeTriggerDisabledException(e)) {
      metrics.counter(MetricConstants.EXECUTION_TRIGGER_DISABLED_METRIC, getClass());
      LOG.info(
          "Time trigger disabled for workflow [{}][{}] triggerUUID=[{}]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid());
      return true;
    } else {
      metrics.counter(
          MetricConstants.EXECUTION_ERROR_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          MetricConstants.TAG_VALUE_UNKNOWN_422);
      LOG.error(
          "Unknown 422 unprocessable for workflow [{}][{}] triggerUUID=[{}]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid(),
          e);
      throw new MaestroRetryableError(
          e,
          "Unknown 422 unprocessable for workflow [%s][%s] triggerUUID=[%s]",
          timeTriggerExecution.getWorkflowId(),
          timeTriggerExecution.getWorkflowVersion(),
          timeTriggerExecution.getWorkflowTriggerUuid());
    }
  }
}
