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
package com.netflix.maestro.engine.handlers;

import static com.netflix.conductor.core.execution.ApplicationException.Code.CONFLICT;

import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Collections;
import lombok.AllArgsConstructor;

/** Workflow runner to run a maestro workflow in conductor. */
@AllArgsConstructor
public class WorkflowRunner {
  private final WorkflowExecutor workflowExecutor;
  private final WorkflowTranslator translator;
  private final WorkflowHelper workflowHelper;

  /**
   * Run a maestro workflow in conductor.
   *
   * @param instance Maestro workflow instance.
   * @return UUID of the conductor workflow instance
   */
  public String start(WorkflowInstance instance) {
    return workflowExecutor.startWorkflow(
        translator.translate(instance),
        Collections.singletonMap(
            Constants.WORKFLOW_SUMMARY_FIELD,
            workflowHelper.createWorkflowSummaryFromInstance(instance)),
        null,
        null,
        String.valueOf(System.currentTimeMillis()), // use event field to keep enqueue time
        Collections.emptyMap());
  }

  /**
   * Stop a running workflow instance.
   *
   * @param executionId internal workflow instance execution id
   * @param reason reason to stop it.
   */
  public void terminate(String executionId, WorkflowInstance.Status status, String reason) {
    try {
      workflowExecutor.terminateWorkflow(executionId, status + "-" + reason);
    } catch (ApplicationException e) {
      if (e.getCode() != CONFLICT) {
        throw e;
      }
    }
  }

  /**
   * Restart a maestro workflow instance in conductor as a new run.
   *
   * @param instance Maestro workflow instance.
   * @return UUID of the conductor workflow instance
   */
  public String restart(WorkflowInstance instance) {
    return workflowExecutor.startWorkflow(
        translator.translate(instance),
        Collections.singletonMap(
            Constants.WORKFLOW_SUMMARY_FIELD,
            workflowHelper.createWorkflowSummaryFromInstance(instance)),
        null,
        null,
        String.valueOf(System.currentTimeMillis()), // use event field to keep dequeue time
        Collections.emptyMap());
  }
}
