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
package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.metrics.Monitors;
import lombok.extern.slf4j.Slf4j;

/** Maestro Workflow Task Runner. */
@Slf4j
public class MaestroWorkflowTaskRunner {
  public void runMaestroTask(
      WorkflowExecutor workflowExecutor,
      Workflow workflow,
      Task task,
      WorkflowSystemTask systemTask) {
    try {
      switch (task.getStatus()) {
        case SCHEDULED:
          systemTask.start(workflow, task, workflowExecutor);
          break;

        case IN_PROGRESS:
          systemTask.execute(workflow, task, workflowExecutor);
          break;
        default:
          break;
      }
    } catch (Exception e) {
      LOG.error(
          "NOT Terminate Maestro step [{}] for the task [{}] in workflow [{}][{}], getting an exception",
          task.getReferenceTaskName(),
          task.getTaskId(),
          workflow.getWorkflowName(),
          workflow.getWorkflowId(),
          e);
      Monitors.error(e.getClass().getSimpleName(), "unexpected_exception_in_maestro_task");
      throw e;
    }
  }
}
