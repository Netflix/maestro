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
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepDependencies;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * maestro start task to work around the conductor start slowness issue and also provides exactly
 * once execution start. It only marks the runtime overview with execution id to associate this
 * conductor workflow execution to maestro workflow instance.
 */
@Slf4j
public class MaestroStartTask extends WorkflowSystemTask {
  /** Special prefix to indicate that the failure is due to dedup and ignore its finalized. */
  public static final String DEDUP_FAILURE_PREFIX = "[DEDUP][IGNORE]";

  /** All step dependencies key. */
  public static final String ALL_STEP_DEPENDENCIES = "all_step_dependencies";

  private final MaestroWorkflowInstanceDao instanceDao;
  private final MaestroStepInstanceDao stepInstanceDao;
  private final ExecutionDAO executionDao;
  private final ObjectMapper objectMapper;
  private static final int INITIAL_RUN_ID = 1;

  /** Constructor. */
  public MaestroStartTask(
      MaestroWorkflowInstanceDao instanceDao,
      MaestroStepInstanceDao stepInstanceDao,
      ExecutionDAO executionDao,
      ObjectMapper objectMapper) {
    super(Constants.DEFAULT_START_STEP_NAME);
    this.instanceDao = instanceDao;
    this.stepInstanceDao = stepInstanceDao;
    this.executionDao = executionDao;
    this.objectMapper = objectMapper;
  }

  /**
   * It provides exactly once execution start. It handles the case that the same instance is started
   * twice in conductor.
   */
  @SuppressWarnings({"PMD.AvoidCatchingNPE"})
  @Override
  public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
    try {
      WorkflowSummary summary =
          StepHelper.retrieveWorkflowSummary(objectMapper, workflow.getInput());

      if (summary.getWorkflowRunId() > INITIAL_RUN_ID) {
        Map<String, Map<StepDependencyType, StepDependencies>> stepDependenciesMap =
            stepInstanceDao.getAllStepDependencies(
                summary.getWorkflowId(),
                summary.getWorkflowInstanceId(),
                summary.getWorkflowRunId() - INITIAL_RUN_ID);
        if (stepDependenciesMap != null) {
          task.getOutputData().put(ALL_STEP_DEPENDENCIES, stepDependenciesMap);
        }
      }

      Optional<Details> details =
          instanceDao.executeWorkflowInstance(summary, workflow.getWorkflowId());
      if (details.isPresent()) {
        LOG.info(
            "Failed to execute maestro task with error {}, will retry it again", details.get());
        task.setStatus(Task.Status.IN_PROGRESS); // retry
      } else {
        LOG.debug(
            "Execute a workflow instance: {} with execution_id: {}",
            summary.getIdentity(),
            workflow.getWorkflowId());
        tryAddDummyTasksForParams(workflow, summary);
        task.setStatus(Task.Status.COMPLETED);
      }
      return true;
    } catch (MaestroInternalError | MaestroNotFoundException | NullPointerException e) {
      // if start task failed, it is fatal error, no retry
      task.setStatus(Task.Status.FAILED_WITH_TERMINAL_ERROR);
      task.setReasonForIncompletion(DEDUP_FAILURE_PREFIX + e.getMessage());
      LOG.error(
          "Error executing Maestro start task: {} in workflow: {}",
          task.getTaskId(),
          workflow.getWorkflowId(),
          e);
      return true;
    }
    // Don't catch unexpected exception and MaestroWorkflowExecutor will retry it.
  }

  private void tryAddDummyTasksForParams(Workflow workflow, WorkflowSummary summary) {
    if (!summary.isFreshRun() && summary.getRunPolicy() != RunPolicy.RESTART_FROM_BEGINNING) {
      Map<String, String> stepUuids =
          stepInstanceDao.getAllLatestStepUuidFromAncestors(
              summary.getWorkflowId(), summary.getWorkflowInstanceId());
      workflow.getWorkflowDefinition().getTaskByRefName(Constants.DEFAULT_START_FORK_STEP_NAME)
          .getForkTasks().stream()
          .flatMap(tasks -> tasks.stream().map(WorkflowTask::getTaskReferenceName))
          .forEach(stepUuids::remove);
      if (!stepUuids.isEmpty()) {
        List<Task> tasks = executionDao.getTasks(new ArrayList<>(stepUuids.values()));
        tasks.forEach(
            task -> {
              task.setWorkflowInstanceId(workflow.getWorkflowId());
              // flag that the task is cloned.
              if (task.getSeq() >= 0) {
                task.setSeq(-(task.getSeq() + 1));
                task.setStatus(Task.Status.SKIPPED);
              }
            });
        if (!tasks.isEmpty()) {
          executionDao.createTasks(tasks);
        }
      }
    }
  }

  @Override
  public boolean isAsync() {
    return true;
  }
}
