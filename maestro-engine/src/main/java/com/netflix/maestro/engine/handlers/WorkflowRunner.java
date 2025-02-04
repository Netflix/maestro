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

import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.utils.IdHelper;
import java.util.Collections;
import lombok.AllArgsConstructor;

/** Workflow runner to run a maestro workflow in the internal flow engine. */
@AllArgsConstructor
public class WorkflowRunner {
  private final FlowOperation flowOperation;
  private final WorkflowTranslator translator;
  private final WorkflowHelper workflowHelper;

  /**
   * Run a maestro workflow in maestro flow engine.
   *
   * @param instance Maestro workflow instance.
   * @return UUID of the internal flow instance
   */
  public String start(WorkflowInstance instance) {
    return flowOperation.startFlow(
        IdHelper.deriveGroupId(instance),
        instance.getWorkflowUuid(),
        instance.getIdentity(),
        translator.translate(instance),
        Collections.singletonMap(
            Constants.WORKFLOW_SUMMARY_FIELD,
            workflowHelper.createWorkflowSummaryFromInstance(instance)));
  }

  /**
   * Restart a maestro workflow instance in maestro flow engine as a new run.
   *
   * @param instance Maestro workflow instance.
   * @return UUID of the internal flow instance
   */
  public String restart(WorkflowInstance instance) {
    return start(instance);
  }
}
