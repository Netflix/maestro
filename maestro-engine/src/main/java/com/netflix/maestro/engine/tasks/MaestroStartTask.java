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
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.runtime.ExecutionPreparer;
import com.netflix.maestro.flow.runtime.FlowTask;
import com.netflix.maestro.models.error.Details;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro start task to provide exactly once execution start. It only marks the runtime overview
 * with execution id to associate this internal flow execution to maestro workflow instance. It can
 * be simplified with the new internal flow engine but not required for now.
 */
@Slf4j
public final class MaestroStartTask implements FlowTask {
  /** Special prefix to indicate that the failure is due to dedup and ignore its finalized. */
  public static final String DEDUP_FAILURE_PREFIX = "[DEDUP][IGNORE]";

  private final MaestroWorkflowInstanceDao instanceDao;
  private final ExecutionPreparer executionPreparer;
  private final ObjectMapper objectMapper;

  /** Constructor. */
  public MaestroStartTask(
      MaestroWorkflowInstanceDao instanceDao,
      ExecutionPreparer executionPreparer,
      ObjectMapper objectMapper) {
    this.instanceDao = instanceDao;
    this.executionPreparer = executionPreparer;
    this.objectMapper = objectMapper;
  }

  /**
   * It provides exactly once execution start. It handles the case that the same instance is started
   * twice in internal flow engine, which might happen in certain rare scenarios.
   */
  @Override
  public boolean execute(Flow flow, Task task) {
    try {
      WorkflowSummary summary = StepHelper.retrieveWorkflowSummary(objectMapper, flow.getInput());

      Optional<Details> details = instanceDao.executeWorkflowInstance(summary, flow.getFlowId());
      if (details.isPresent()) {
        LOG.info(
            "Failed to execute maestro task with error {}, will retry it again", details.get());
        task.setStatus(Task.Status.IN_PROGRESS); // retry
      } else {
        LOG.debug(
            "Execute a workflow instance: {} with execution_id: {}",
            summary.getIdentity(),
            flow.getFlowId());
        executionPreparer.addExtraTasksAndInput(flow, task);
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
          flow.getFlowId(),
          e);
      return true;
    }
    // Don't catch unexpected exception and the flow engine will retry it.
  }
}
