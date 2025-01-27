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

import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.AbstractStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.utils.Checks;
import java.util.Collections;
import lombok.NoArgsConstructor;

/** Step translator to transform maestro step to internal flow TaskDef. */
@NoArgsConstructor
public class StepTranslator implements Translator<Step, TaskDef> {
  @Override
  public TaskDef translate(Step step) {
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

  private TaskDef createMaestroGateTask(JoinStep step) {
    return new TaskDef(step.getId(), step.getType().name(), null, step.getJoinOn());
  }

  private TaskDef createMaestroTask(AbstractStep step) {
    if (step.getParams() == null) {
      step.setParams(Collections.emptyMap());
    }
    if (step.getFailureMode() == null) { // if unset, using the default
      step.setFailureMode(Defaults.DEFAULT_FAILURE_MODE);
    }
    return new TaskDef(
        step.getId(),
        Constants.MAESTRO_TASK_NAME,
        Collections.singletonMap(Constants.STEP_DEFINITION_FIELD, step),
        null);
  }
}
