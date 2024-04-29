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
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import java.util.UUID;
import lombok.NoArgsConstructor;

/** Step translator to transform maestro step to conductor WorkflowTask. */
@NoArgsConstructor
public class StepTranslator implements Translator<Step, WorkflowTask> {
  @Override
  public WorkflowTask translate(Step step) {
    Checks.notNull(step, "Cannot find step in the definition");
    switch (step.getType()) {
      case JOIN:
        return createMaestroGateTask((JoinStep) step);
      case FOREACH:
      case SUBWORKFLOW:
      case NOOP:
      case SLEEP:
      case TITUS:
      case NOTEBOOK:
        return createMaestroTask((AbstractStep) step);
      case TEMPLATE:
      default:
        throw new UnsupportedOperationException(
            step.getType() + " step type is not implemented yet.");
    }
  }

  private WorkflowTask createMaestroGateTask(JoinStep step) {
    WorkflowTask task = new WorkflowTask();
    task.setType(TaskType.TASK_TYPE_EXCLUSIVE_JOIN);
    task.setName(UUID.randomUUID().toString());
    task.setDescription(step.getDescription());
    task.setTaskReferenceName(step.getId());
    task.setJoinOn(step.getJoinOn());
    return task;
  }

  private WorkflowTask createMaestroTask(AbstractStep step) {
    if (step.getParams() == null) {
      step.setParams(Collections.emptyMap());
    }
    WorkflowTask task = new WorkflowTask();
    task.setType(Constants.MAESTRO_TASK_NAME);
    task.setName(UUID.randomUUID().toString());
    task.setDescription(step.getDescription());
    task.setTaskReferenceName(step.getId());
    task.setTaskDefinition(createMaestroTaskDef(step));
    if (step.getFailureMode() == null) { // if unset, using the default
      step.setFailureMode(Defaults.DEFAULT_FAILURE_MODE);
    }
    task.setInputParameters(
        Collections.singletonMap(Constants.STEP_DEFINITION_FIELD, StepHelper.wrap(step)));
    return task;
  }

  private TaskDef createMaestroTaskDef(AbstractStep step) {
    TaskDef taskDef = new TaskDef();
    taskDef.setName(step.getId());
    taskDef.setConcurrentExecLimit(0); // not using conductor step concurrency feature
    taskDef.setTimeoutPolicy(TaskDef.TimeoutPolicy.RETRY);
    taskDef.setTimeoutSeconds(0); // Not using conductor step timeout
    taskDef.setRetryLogic(TaskDef.RetryLogic.CUSTOM);
    taskDef.setRetryCount(CONDUCTOR_RETRY_NUM); // use a super large number
    taskDef.setRetryDelaySeconds(CONDUCTOR_RETRY_DELAY);
    taskDef.setResponseTimeoutSeconds(CONDUCTOR_RESPONSE_TIMEOUT);
    return taskDef;
  }
}
