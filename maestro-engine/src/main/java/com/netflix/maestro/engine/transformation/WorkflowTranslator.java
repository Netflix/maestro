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
package com.netflix.maestro.engine.transformation;

import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.List;
import lombok.AllArgsConstructor;

/** Workflow translator to transform maestro workflow to internal FlowDef. */
@AllArgsConstructor
public class WorkflowTranslator implements Translator<WorkflowInstance, FlowDef> {
  private final StepTranslator stepTranslator;

  @Override
  public FlowDef translate(WorkflowInstance instance) {
    WorkflowGraph wfGraph =
        WorkflowGraph.build(instance.getRuntimeWorkflow(), instance.getRuntimeDag());

    // Algorithm to go over the graph to create tasks
    List<List<TaskDef>> tasks = wfGraph.computePaths(stepTranslator);

    // prepareTask, monitorTask, and a list of lists of task nodes
    TaskDef prepare =
        new TaskDef(
            Constants.DEFAULT_START_TASK_NAME,
            Constants.DEFAULT_START_TASK_NAME,
            null,
            null); // start task
    TaskDef monitor =
        new TaskDef(
            Constants.DEFAULT_END_TASK_NAME,
            Constants.DEFAULT_END_TASK_NAME,
            null,
            wfGraph.getEndNodeIds());

    FlowDef flow = new FlowDef();
    flow.setPrepareTask(prepare);
    flow.setMonitorTask(monitor);
    flow.setTasks(tasks);
    long timeoutInMillis =
        ObjectHelper.valueOrDefault(
            instance.getTimeoutInMillis(), Defaults.DEFAULT_TIME_OUT_LIMIT_IN_MILLIS);
    flow.setTimeoutInMillis(timeoutInMillis);
    // Enable final flow status callback.
    flow.setFinalFlowStatusCallbackEnabled(true);
    return flow;
  }
}
