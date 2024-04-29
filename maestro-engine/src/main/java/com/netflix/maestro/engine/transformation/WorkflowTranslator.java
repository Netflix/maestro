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

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.maestro.engine.utils.ObjectHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;

/** Workflow translator to transform maestro workflow to conductor WorkflowDef. */
@AllArgsConstructor
public class WorkflowTranslator implements Translator<WorkflowInstance, WorkflowDef> {
  private final StepTranslator stepTranslator;

  @Override
  public WorkflowDef translate(WorkflowInstance instance) {
    WorkflowGraph wfGraph =
        WorkflowGraph.build(instance.getRuntimeWorkflow(), instance.getRuntimeDag());

    // Algorithm to go over the graph to create tasks
    List<List<WorkflowTask>> tasks = wfGraph.computePaths(stepTranslator);

    // start, startFork, and end accessory nodes
    WorkflowTask start = new WorkflowTask(); // start task
    start.setType(Constants.DEFAULT_START_STEP_NAME);
    start.setName(Constants.DEFAULT_START_STEP_NAME);
    start.setTaskReferenceName(Constants.DEFAULT_START_STEP_NAME);
    TaskDef taskDef = new TaskDef();
    taskDef.setName(Constants.DEFAULT_START_STEP_NAME);
    taskDef.setTimeoutPolicy(TaskDef.TimeoutPolicy.RETRY);
    taskDef.setTimeoutSeconds(0); // Not using conductor step timeout
    taskDef.setRetryCount(CONDUCTOR_RETRY_NUM);
    taskDef.setRetryDelaySeconds(CONDUCTOR_RETRY_DELAY);
    taskDef.setResponseTimeoutSeconds(CONDUCTOR_RESPONSE_TIMEOUT);
    start.setTaskDefinition(taskDef);

    WorkflowTask startFork = new WorkflowTask(); // start forkTask
    startFork.setType(TaskType.TASK_TYPE_FORK_JOIN);
    startFork.setName(Constants.DEFAULT_START_FORK_STEP_NAME);
    startFork.setTaskReferenceName(Constants.DEFAULT_START_FORK_STEP_NAME);
    startFork.getForkTasks().addAll(tasks);

    WorkflowTask end = new WorkflowTask();
    end.setType(TaskType.TASK_TYPE_JOIN);
    end.setName(Constants.DEFAULT_END_STEP_NAME);
    end.setTaskReferenceName(Constants.DEFAULT_END_STEP_NAME);
    end.setJoinOn(wfGraph.getEndNodeIds());

    WorkflowDef workflowDef = new WorkflowDef();
    workflowDef.setOwnerApp(Constants.MAESTRO_QUALIFIER);
    workflowDef.setCreatedBy(Constants.MAESTRO_QUALIFIER);

    workflowDef.setName(instance.getWorkflowUuid());

    workflowDef.setTasks(Arrays.asList(start, startFork, end));
    long timeoutInMillis =
        ObjectHelper.valueOrDefault(
            instance.getTimeoutInMillis(), Defaults.DEFAULT_TIME_OUT_LIMIT_IN_MILLIS);
    workflowDef.setTimeoutSeconds(TimeUnit.MILLISECONDS.toSeconds(timeoutInMillis));
    workflowDef.setTimeoutPolicy(WorkflowDef.TimeoutPolicy.TIME_OUT_WF);
    // Enable workflow status listener.
    workflowDef.setWorkflowStatusListenerEnabled(true);
    return workflowDef;
  }
}
