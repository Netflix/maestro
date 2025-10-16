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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.db.StepAction;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.engine.tracing.MaestroTracingManager;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** step runtime manager to manage step runtime and their results. */
@SuppressWarnings("PMD.ExhaustiveSwitchHasDefault")
public final class StepRuntimeManager {
  private final Map<StepType, StepRuntime> stepRuntimeMap;
  private final ObjectMapper objectMapper;
  private final ParamsManager paramsManager;
  private final MaestroMetrics metrics;
  private final MaestroTracingManager tracingManager;

  /** StepRuntime manager constructor. */
  public StepRuntimeManager(
      Map<StepType, StepRuntime> stepRuntimeMap,
      ObjectMapper objectMapper,
      ParamsManager paramsManager,
      MaestroMetrics metricRepo,
      MaestroTracingManager tracingManager) {
    this.stepRuntimeMap = Collections.unmodifiableMap(stepRuntimeMap);
    this.objectMapper = objectMapper;
    this.paramsManager = paramsManager;
    this.metrics = metricRepo;
    this.tracingManager = tracingManager;
  }

  private StepRuntime getStepRuntime(StepType type) {
    return Checks.notNull(stepRuntimeMap.get(type), "Cannot found the step type %s", type);
  }

  private StepRuntimeSummary cloneSummary(StepRuntimeSummary summary) {
    StepAction latestAction = summary.getPendingAction();
    StepRuntimeSummary cloned = objectMapper.convertValue(summary, StepRuntimeSummary.class);
    cloned.setPendingAction(latestAction);
    summary.setPendingAction(null); // clear the pending action after passing it to step runtime
    return cloned;
  }

  private void markTerminatedWithMetric(
      StepRuntimeSummary runtimeSummary, StepRuntime.State stepState, StepInstance.Status status) {
    metrics.counter(
        MetricConstants.ON_STEP_TERMINATED,
        getClass(),
        MetricConstants.STEP_STATE_TAG,
        stepState.name(),
        MetricConstants.STEP_STATUS_TAG,
        status.name());
    runtimeSummary.markTerminated(status, tracingManager);
  }

  private StepInstance.Status getPlatformErrorStatus(StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary.getStepRetry().hasReachedPlatformRetryLimit()) {
      return StepInstance.Status.FATALLY_FAILED;
    } else {
      return StepInstance.Status.PLATFORM_FAILED;
    }
  }

  private StepInstance.Status getUserErrorStatus(StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary.getStepRetry().hasReachedErrorRetryLimit()) {
      return StepInstance.Status.FATALLY_FAILED;
    } else {
      return StepInstance.Status.USER_FAILED;
    }
  }

  /**
   * run step runtime start() and parse its result and update runtime summary.
   *
   * @param workflowSummary readonly workflow summary
   * @param runtimeSummary step runtime summary
   * @return flag, true means ready to persist the changes in the result
   */
  public boolean start(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    StepRuntime.Result result =
        getStepRuntime(runtimeSummary.getType())
            .start(workflowSummary, step, cloneSummary(runtimeSummary));
    runtimeSummary.mergeRuntimeUpdate(result.timeline(), result.artifacts());
    runtimeSummary.setNextPollingDelayInMillis(result.nextPollingDelayInMillis());
    switch (result.state()) {
      case CONTINUE:
      case DONE:
        runtimeSummary.markExecuting(tracingManager);
        return result.shouldPersist();
      case USER_ERROR:
        markTerminatedWithMetric(
            runtimeSummary, result.state(), getUserErrorStatus(runtimeSummary));
        return false;
      case PLATFORM_ERROR:
        markTerminatedWithMetric(
            runtimeSummary, result.state(), getPlatformErrorStatus(runtimeSummary));
        return false;
      case FATAL_ERROR:
        markTerminatedWithMetric(
            runtimeSummary, result.state(), StepInstance.Status.FATALLY_FAILED);
        return false;
      case STOPPED:
        markTerminatedWithMetric(runtimeSummary, result.state(), StepInstance.Status.STOPPED);
        return false;
      case TIMED_OUT:
        markTerminatedWithMetric(runtimeSummary, result.state(), StepInstance.Status.TIMED_OUT);
        return false;
      default:
        throw new MaestroInternalError(
            "Entered an unexpected result state [%s] for step %s when starting",
            result.state(), runtimeSummary.getIdentity());
    }
  }

  /**
   * run step runtime execute() and parse its result and update runtime summary.
   *
   * @param workflowSummary readonly workflow summary
   * @param runtimeSummary step runtime summary
   * @return flag, true means ready to persist the changes in the result
   */
  public boolean execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    StepRuntime.Result result =
        getStepRuntime(runtimeSummary.getType())
            .execute(workflowSummary, step, cloneSummary(runtimeSummary));
    runtimeSummary.mergeRuntimeUpdate(result.timeline(), result.artifacts());
    runtimeSummary.setNextPollingDelayInMillis(result.nextPollingDelayInMillis());
    switch (result.state()) {
      case CONTINUE:
        return true;
      case DONE:
        runtimeSummary.markFinishing(tracingManager);
        return result.shouldPersist();
      case USER_ERROR:
        markTerminatedWithMetric(
            runtimeSummary, result.state(), getUserErrorStatus(runtimeSummary));
        return false;
      case PLATFORM_ERROR:
        markTerminatedWithMetric(
            runtimeSummary, result.state(), getPlatformErrorStatus(runtimeSummary));
        return false;
      case FATAL_ERROR:
        markTerminatedWithMetric(
            runtimeSummary, result.state(), StepInstance.Status.FATALLY_FAILED);
        return false;
      case STOPPED:
        markTerminatedWithMetric(runtimeSummary, result.state(), StepInstance.Status.STOPPED);
        return false;
      case TIMED_OUT:
        markTerminatedWithMetric(runtimeSummary, result.state(), StepInstance.Status.TIMED_OUT);
        return false;
      default:
        throw new MaestroInternalError(
            "Entered an unexpected result state [%s] for step %s when executing",
            result.state(), runtimeSummary.getIdentity());
    }
  }

  /**
   * merge parameters in step definition and step runtime.
   *
   * <p>It includes all runtime params from step definition and step runtime injection and
   * conditional params.
   *
   * @param stepDefinition step definition
   * @param workflowSummary workflow summary
   * @param runtimeSummary step runtime summary
   * @return all step parameters available at runtime to be evaluated
   */
  public Map<String, Parameter> getAllParams(
      Step stepDefinition, WorkflowSummary workflowSummary, StepRuntimeSummary runtimeSummary) {
    return paramsManager.generateMergedStepParams(
        workflowSummary, stepDefinition, getStepRuntime(stepDefinition.getType()), runtimeSummary);
  }

  /**
   * run step runtime terminate() and update runtime summary.
   *
   * @param workflowSummary readonly workflow summary
   * @param runtimeSummary step runtime summary
   */
  public void terminate(
      WorkflowSummary workflowSummary,
      StepRuntimeSummary runtimeSummary,
      StepInstance.Status status) {
    try {
      StepRuntime.Result result =
          getStepRuntime(runtimeSummary.getType())
              .terminate(workflowSummary, cloneSummary(runtimeSummary));
      Checks.checkTrue(
          result.state() == StepRuntime.State.STOPPED,
          "terminate call should return a STOPPED state in result: %s",
          result);
      runtimeSummary.mergeRuntimeUpdate(result.timeline(), result.artifacts());
      runtimeSummary.markTerminated(status, tracingManager);
    } catch (RuntimeException e) {
      metrics.counter(
          MetricConstants.STEP_RUNTIME_MANAGER_TERMINATE_EXCEPTION,
          getClass(),
          MetricConstants.STATUS_TAG,
          status.name());
      runtimeSummary.addTimeline(
          TimelineLogEvent.warn(
              "Failed to terminate the step %s of the workflow %s because [%s]",
              runtimeSummary.getIdentity(), workflowSummary.getIdentity(), e.getMessage()));
      throw e;
    }
  }

  /** get runtime tags from step runtime. */
  public List<Tag> getRuntimeTags(Step stepDefinition) {
    return getStepRuntime(stepDefinition.getType()).injectRuntimeTags();
  }
}
