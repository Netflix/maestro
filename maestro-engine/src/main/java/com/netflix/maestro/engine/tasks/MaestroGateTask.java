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
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.SystemTaskType;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.TaskHelper;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro Gate task is a gate step to wait for upstream joinOn steps complete. It makes the
 * execution following the order defined in the DAG of the Maestro workflow definition.
 *
 * <p>It's implementation is similar to Conductor Join task. We override the default conductor one
 * to optimize the logic for maestro.
 *
 * <p>Here, we overwrite the default conductor EXCLUSIVE_JOIN task to reuse its task mapper.
 */
@Slf4j
public class MaestroGateTask extends WorkflowSystemTask {

  private final MaestroStepInstanceDao stepInstanceDao;
  private final ObjectMapper objectMapper;

  /** Constructor. */
  public MaestroGateTask(MaestroStepInstanceDao stepInstanceDao, ObjectMapper objectMapper) {
    // Overwrite the conductor exclusive join task with maestro optimized gate join logic
    super(SystemTaskType.EXCLUSIVE_JOIN.name());
    this.stepInstanceDao = stepInstanceDao;
    this.objectMapper = objectMapper;
  }

  /**
   * The execution is not expected to throw an exception. If it happens, the exception will be
   * handled by the upstream caller (i.e. MaestroWorkflowExecutor will retry).
   */
  @Override
  public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
    Map<String, Task> taskMap = TaskHelper.getTaskMap(workflow);
    Optional<Task.Status> done = executeJoin(task, taskMap);
    if (done.isPresent() && confirmDone(workflow, task)) { // update task status if it is done
      task.setStatus(done.get());
      return true;
    }
    return false;
  }

  private boolean confirmDone(Workflow workflow, Task task) {
    List<String> joinOn = getJoinOnSteps(task);
    LOG.debug("Confirming steps [{}] are actually completed.", joinOn);
    WorkflowSummary workflowSummary =
        StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());

    Map<String, StepRuntimeState> status =
        stepInstanceDao.getStepStates(
            workflowSummary.getWorkflowId(),
            workflowSummary.getWorkflowInstanceId(),
            workflowSummary.getWorkflowRunId(),
            joinOn);
    for (String joinOnRef : joinOn) {
      StepRuntimeState state = status.get(joinOnRef);
      if (state == null || !state.getStatus().isComplete()) {
        LOG.info(
            "Steps [{}] is not completed yet although the task status is done. Will try the task [{}] again.",
            joinOnRef,
            task.getTaskId());
        return false;
      }
    }
    LOG.debug("Confirmed steps [{}] are actually completed.", joinOn);
    return true;
  }

  /**
   * Optimized conductor join task logic for maestro.
   *
   * @param task conductor task
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
      hasFailures = !taskStatus.isSuccessful() && !forkedTask.getWorkflowTask().isOptional();
      if (!taskStatus.isTerminal()) {
        allDone = false;
      }
      if (hasFailures) {
        break;
      }
    }
    LOG.trace(
        "Gate task {} is blocked by its joinOn tasks: {} with result allDone: {}, hasFailures: {}",
        task.getReferenceTaskName(),
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

  @SuppressWarnings("unchecked")
  private List<String> getJoinOnSteps(Task task) {
    return (List<String>) task.getInputData().get("joinOn");
  }
}
