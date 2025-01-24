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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.transformation.Translator;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/** Utility class for maestro task. */
@Slf4j
public final class TaskHelper {
  private TaskHelper() {}

  /** Task is a user defined task type. */
  static boolean isUserDefinedTask(Task task) {
    return Constants.USER_DEFINED_TASKS.contains(task.getTaskType());
  }

  /** Task is a real task instead of a placeholder task for params or restart. */
  static boolean isRealTask(Task task) {
    return task.getSeq() >= 0 && task.isActive(); // NOT_CREATED steps are all inactive
  }

  /** Task is a user defined task type, excluding cloned dummy tasks. */
  static boolean isUserDefinedRealTask(Task task) {
    return isUserDefinedTask(task) && isRealTask(task);
  }

  /** Task is a complete task including evaluated parameter data. */
  private static boolean isValidTaskWithParamData(Task task) {
    return task.getStatus().isSuccessful()
        && task.getStatus().isTerminal()
        && isUserDefinedTask(task);
  }

  /** utility method to get internal task mapping from workflow data. */
  public static Map<String, Task> getTaskMap(Flow flow) {
    return flow.getFinishedTasks().stream()
        .filter(t -> !StepType.JOIN.name().equals(t.getTaskType()) && t.getSeq() >= 0)
        .collect(
            Collectors.toMap(
                Task::referenceTaskName,
                Function.identity(),
                // it includes retried tasks so pick the last one
                (task1, task2) -> task2));
  }

  /** utility method to get internal task output data mapping from internal flow data. */
  public static Map<String, Map<String, Object>> getAllStepOutputData(Flow flow) {
    return flow.getFinishedTasks().stream()
        // step should be in terminal state and normal tasks in DAG
        .filter(TaskHelper::isValidTaskWithParamData)
        .collect(
            Collectors.toMap(
                Task::referenceTaskName,
                Task::getOutputData,
                // it includes retried tasks so pick the last one
                (task1, task2) -> task2));
  }

  /** utility method to get real maestro task mapping from internal task data. */
  public static Map<String, Task> getUserDefinedRealTaskMap(Stream<Task> allTasks) {
    return allTasks
        .filter(TaskHelper::isUserDefinedRealTask)
        .collect(
            Collectors.toMap(
                Task::referenceTaskName,
                Function.identity(),
                // it includes retried tasks so pick the last one
                (task1, task2) -> task2));
  }

  /**
   * It translates known step states into a compact version of the step status map. It needs the
   * step runtime dag info (in a linked hashmap) to compress the data, i.e. using the ordinal id
   * instead of step id as the id, and also use it to fill the not created steps into the map.
   *
   * @param summary workflow summary including runtime dag.
   * @param states step id to its runtime state map
   * @return a compact step status map
   */
  public static EnumMap<StepInstance.Status, WorkflowStepStatusSummary> toStepStatusMap(
      WorkflowSummary summary, Map<String, StepRuntimeState> states) {
    AtomicLong ordinal = new AtomicLong(0);
    Map<String, Long> stepOrdinalMap =
        summary.getRuntimeDag().keySet().stream()
            .collect(Collectors.toMap(Function.identity(), s -> ordinal.incrementAndGet()));

    EnumMap<StepInstance.Status, WorkflowStepStatusSummary> stats =
        new EnumMap<>(StepInstance.Status.class);
    states.forEach(
        (stepId, state) -> {
          if (state.getStatus() != StepInstance.Status.NOT_CREATED) {
            if (!stats.containsKey(state.getStatus())) {
              stats.put(state.getStatus(), new WorkflowStepStatusSummary());
            }
            List<Long> stepInfo =
                Arrays.asList(
                    stepOrdinalMap.remove(stepId), state.getStartTime(), state.getEndTime());
            stats.get(state.getStatus()).addStep(stepInfo);
          }
        });

    stats.forEach((status, stepStatusSummary) -> stepStatusSummary.sortSteps());
    // Don't include NOT_CREATED in the stats. Use decode method to get them if needed.
    return stats;
  }

  /** utility method to compute workflow runtime overview from task data. */
  public static WorkflowRuntimeOverview computeOverview(
      ObjectMapper objectMapper,
      WorkflowSummary summary,
      WorkflowRollupOverview rollupBase,
      Map<String, Task> realTaskMap) {

    Map<String, StepRuntimeState> states =
        realTaskMap.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        StepHelper.retrieveStepRuntimeState(
                            e.getValue().getOutputData(), objectMapper)));

    EnumMap<StepInstance.Status, WorkflowStepStatusSummary> stepStatusMap =
        toStepStatusMap(summary, states);

    WorkflowRollupOverview rollupOverview =
        realTaskMap.values().stream()
            .filter(t -> t.getOutputData().containsKey(Constants.STEP_RUNTIME_SUMMARY_FIELD))
            .map(
                t -> {
                  StepRuntimeSummary stepSummary =
                      StepHelper.retrieveRuntimeSummary(objectMapper, t.getOutputData());
                  switch (stepSummary.getType()) {
                    case FOREACH:
                      if (stepSummary.getArtifacts().containsKey(Artifact.Type.FOREACH.key())) {
                        ForeachArtifact artifact =
                            stepSummary.getArtifacts().get(Artifact.Type.FOREACH.key()).asForeach();
                        if (artifact.getForeachOverview() != null
                            && artifact.getForeachOverview().getCheckpoint() > 0) {
                          return artifact.getForeachOverview().getOverallRollup();
                        }
                      }
                      break;
                    case SUBWORKFLOW:
                      if (stepSummary.getArtifacts().containsKey(Artifact.Type.SUBWORKFLOW.key())) {
                        SubworkflowArtifact artifact =
                            stepSummary
                                .getArtifacts()
                                .get(Artifact.Type.SUBWORKFLOW.key())
                                .asSubworkflow();
                        if (artifact.getSubworkflowOverview() != null) {
                          return artifact.getSubworkflowOverview().getRollupOverview();
                        }
                      }
                      break;
                    case TEMPLATE:
                    default:
                      break;
                  }

                  StepInstance.Status status = StepHelper.retrieveStepStatus(t.getOutputData());
                  WorkflowRollupOverview.CountReference ref =
                      new WorkflowRollupOverview.CountReference();
                  ref.setCnt(1);
                  if (status.isOverview()) {
                    ref.setRef(
                        Collections.singletonMap(
                            RollupAggregationHelper.getReference(
                                summary.getWorkflowId(), summary.getWorkflowRunId()),
                            Collections.singletonList(
                                RollupAggregationHelper.getReference(
                                    summary.getWorkflowInstanceId(),
                                    stepSummary.getStepId(),
                                    stepSummary.getStepAttemptId()))));
                  }
                  return WorkflowRollupOverview.of(1L, Collections.singletonMap(status, ref));
                })
            .reduce(new WorkflowRollupOverview(), WorkflowRollupOverview::aggregate);

    rollupOverview.aggregate(rollupBase);

    return WorkflowRuntimeOverview.of(summary.getTotalStepCount(), stepStatusMap, rollupOverview);
  }

  /**
   * Check if the workflow is completed by checking maestro step status.
   *
   * @param realTaskMap task map to include only real tasks for maestro step
   * @param summary workflow summary
   * @param overview workflow runtime overview
   * @param isFinal flag whether this is the final progress check or periodical check
   * @return either failed or completed status, empty means some jobs are still running
   */
  @SuppressWarnings({"checkstyle:OperatorWrap"})
  public static Optional<Task.Status> checkProgress(
      Map<String, Task> realTaskMap,
      WorkflowSummary summary,
      WorkflowRuntimeOverview overview,
      boolean isFinal) {
    boolean allDone = true;
    boolean isFailed = false; // highest order
    boolean isTimeout = false;
    boolean isStopped = false; // lowest order
    boolean allTerminal =
        isFinal && realTaskMap.values().stream().allMatch(task -> task.getStatus().isTerminal());

    for (Map.Entry<StepInstance.Status, WorkflowStepStatusSummary> entry :
        overview.getStepOverview().entrySet()) {
      if (entry.getKey() != StepInstance.Status.NOT_CREATED && entry.getValue().getCnt() > 0) {
        if (!entry.getKey().isTerminal() || (!allTerminal && entry.getKey().isRetryable())) {
          allDone = false;
          break;
        } else if (entry.getKey() == StepInstance.Status.FATALLY_FAILED
            || entry.getKey() == StepInstance.Status.INTERNALLY_FAILED
            || entry.getKey() == StepInstance.Status.USER_FAILED
            || entry.getKey() == StepInstance.Status.PLATFORM_FAILED
            || entry.getKey() == StepInstance.Status.TIMEOUT_FAILED) {
          isFailed = true;
        } else if (entry.getKey() == StepInstance.Status.TIMED_OUT) {
          isTimeout = true;
        } else if (entry.getKey() == StepInstance.Status.STOPPED) {
          isStopped = true;
        }
      }
    }

    if (allDone && overview.existsNotCreatedStep()) {
      allDone = confirmDone(realTaskMap, summary);
    }

    // It's unexpected. Can happen if the flow fails the run before running maestro task logic
    if (allDone && !isFailed && !isTimeout && !isStopped && !overview.existsCreatedStep()) {
      LOG.error(
          "There are no created steps in the workflow [{}] and mark it as failed.",
          summary.getIdentity());
      isFailed = true;
    }

    LOG.trace(
        "Check task status: done [{}] and with flags: [isFailed: {}], [isTimeout: {}], [isStopped: {}] "
            + "with real task map: [{}] and workflow summary: [{}]",
        allDone,
        isFailed,
        isTimeout,
        isStopped,
        realTaskMap,
        summary);
    if (allDone) {
      if (isFailed) {
        return Optional.of(Task.Status.FAILED);
      } else if (isTimeout) {
        return Optional.of(Task.Status.TIMED_OUT);
      } else if (isStopped) {
        return Optional.of(Task.Status.CANCELED);
      } else {
        // Use this special status to indicate workflow succeeded.
        // So all dummy (NOT_CREATED) tasks will be cancelled.
        return Optional.of(Task.Status.FAILED_WITH_TERMINAL_ERROR);
      }
    }
    return Optional.empty();
  }

  private static boolean confirmDone(Map<String, Task> realTaskMap, WorkflowSummary summary) {
    Map<String, StepTransition> runtimeDag = summary.getRuntimeDag();

    Map<String, Boolean> taskStatusMap =
        realTaskMap.values().stream()
            .filter(t -> t.getStatus().isTerminal()) // ignore non-terminal
            .collect(Collectors.toMap(Task::referenceTaskName, t -> t.getStatus().isSuccessful()));
    return DagHelper.isDone(runtimeDag, taskStatusMap, summary.getRestartConfig());
  }

  /** From Maestro step instance status to internal flow task status. */
  public static void deriveTaskStatus(Task task, StepRuntimeSummary runtimeSummary) {
    switch (runtimeSummary.getRuntimeState().getStatus()) {
      case NOT_CREATED:
      case CREATED:
      case INITIALIZED:
      case PAUSED:
      case WAITING_FOR_SIGNALS:
      case EVALUATING_PARAMS:
      case WAITING_FOR_PERMITS:
      case STARTING:
      case RUNNING:
      case FINISHING:
        task.setStatus(Task.Status.IN_PROGRESS);
        break;
      case DISABLED:
      case UNSATISFIED:
      case SKIPPED:
      case SUCCEEDED:
      case COMPLETED_WITH_ERROR:
        task.setStatus(Task.Status.COMPLETED);
        break;
      case USER_FAILED:
      case PLATFORM_FAILED:
      case TIMEOUT_FAILED:
        task.setStatus(Task.Status.FAILED);
        task.setStartDelayInSeconds(
            runtimeSummary
                .getStepRetry()
                .getNextRetryDelay(runtimeSummary.getRuntimeState().getStatus()));
        break;
      case FATALLY_FAILED:
      case INTERNALLY_FAILED:
        task.setStatus(Task.Status.FAILED);
        task.setStartDelayInSeconds(Translator.DEFAULT_FLOW_TASK_DELAY);
        break;
      case STOPPED:
        task.setStatus(Task.Status.CANCELED);
        break;
      case TIMED_OUT:
        task.setStatus(Task.Status.TIMED_OUT);
        break;
      default:
        throw new MaestroInternalError(
            "Entered an unexpected state [%s] for step %s",
            runtimeSummary.getRuntimeState().getStatus(), runtimeSummary.getIdentity());
    }
    if (task.getStatus().isTerminal()) {
      task.setEndTime(runtimeSummary.getRuntimeState().getEndTime());
    }
  }
}
