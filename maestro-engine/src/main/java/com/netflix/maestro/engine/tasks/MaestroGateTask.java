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
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.transformation.Translator;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.runtime.FlowTask;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro Gate task is a gate step to wait for upstream joinOn steps complete. It makes the
 * execution following the order defined in the DAG of the Maestro workflow definition.
 */
@Slf4j
public class MaestroGateTask implements FlowTask {

  private final MaestroStepInstanceDao stepInstanceDao;
  private final ObjectMapper objectMapper;

  /** Constructor. */
  public MaestroGateTask(MaestroStepInstanceDao stepInstanceDao, ObjectMapper objectMapper) {
    this.stepInstanceDao = stepInstanceDao;
    this.objectMapper = objectMapper;
  }

  /**
   * The execution is not expected to throw an exception. If it happens, the exception will be
   * handled by the upstream caller (i.e., the flow engine will retry).
   */
  @Override
  public boolean execute(Flow flow, Task task) {
    Map<String, Task> taskMap = TaskHelper.getTaskMap(flow);
    Optional<Task.Status> done = executeJoin(task, taskMap);
    if (done.isPresent() && confirmDone(flow, task, false) && confirmDone(flow, task, true)) {
      // update task status if it is done
      task.setStatus(done.get());
      return true;
    }
    task.setStartDelayInSeconds(Translator.DEFAULT_FLOW_TASK_RECONCILIATION_INTERVAL);
    return false;
  }

  private boolean confirmDone(Flow flow, Task task, boolean isStepData) {
    List<String> joinOn = getJoinOnSteps(task);
    LOG.debug(
        "Confirming steps [{}] are actually completed in the flow [{}] with flag isStepData=[{}]",
        joinOn,
        flow.getReference(),
        isStepData);
    Map<String, StepRuntimeState> status;
    if (isStepData) {
      WorkflowSummary workflowSummary =
          StepHelper.retrieveWorkflowSummary(objectMapper, flow.getInput());
      status =
          stepInstanceDao.getStepStates(
              workflowSummary.getWorkflowId(),
              workflowSummary.getWorkflowInstanceId(),
              workflowSummary.getWorkflowRunId(),
              joinOn);
    } else {
      status =
          TaskHelper.getTaskMap(flow).entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      e ->
                          StepHelper.retrieveStepRuntimeState(
                              e.getValue().getOutputData(), objectMapper)));
    }

    for (String joinOnRef : joinOn) {
      StepRuntimeState state = status.get(joinOnRef);
      if (state == null || !state.getStatus().isComplete()) {
        if (isStepData) {
          LOG.warn(
              "Steps [{}] is not completed yet although the task status is done. Will try the flow task [{}][{}] again.",
              joinOnRef,
              flow.getReference(),
              task.referenceTaskName());
        } else {
          LOG.debug(
              "Steps [{}] is not completed yet. Will try the flow task [{}][{}] again.",
              joinOnRef,
              flow.getReference(),
              task.referenceTaskName());
        }
        return false;
      }
    }
    LOG.debug(
        "Confirmed steps [{}] are actually completed in the flow [{}].",
        joinOn,
        flow.getReference());
    return true;
  }

  /**
   * Check if pre-requisite tasks are all done.
   *
   * @param task flow task
   * @param taskMap task mapper
   * @return either failed or completed status, empty means forked jobs are still running
   */
  Optional<Task.Status> executeJoin(Task task, Map<String, Task> taskMap) {
    boolean allDone = true;
    boolean hasFailures = false;
    List<String> joinOn = getJoinOnSteps(task);
    for (String joinOnRef : joinOn) {
      Task forkedTask = taskMap.get(joinOnRef);
      if (forkedTask == null) {
        // Task is not even scheduled yet
        allDone = false;
        break;
      }
      Task.Status taskStatus = forkedTask.getStatus();
      hasFailures = !taskStatus.isSuccessful();
      if (!taskStatus.isTerminal()) {
        allDone = false;
      }
      if (hasFailures) {
        break;
      }
    }
    LOG.trace(
        "Gate task {} is blocked by its joinOn tasks: {} with result allDone: {}, hasFailures: {}",
        task.referenceTaskName(),
        joinOn,
        allDone,
        hasFailures);
    if (allDone || hasFailures) {
      if (hasFailures) {
        return Optional.of(Task.Status.FAILED);
      } else {
        return Optional.of(Task.Status.COMPLETED);
      }
    }
    return Optional.empty();
  }

  private List<String> getJoinOnSteps(Task task) {
    return task.getTaskDef().joinOn();
  }
}
