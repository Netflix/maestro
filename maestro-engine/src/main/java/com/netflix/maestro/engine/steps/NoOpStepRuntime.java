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
package com.netflix.maestro.engine.steps;

import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.definition.Step;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Maestro noop step runtime. */
@Slf4j
@NoArgsConstructor
public class NoOpStepRuntime implements StepRuntime {

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    LOG.info(
        "NoOp step {} in workflow {} execute and then complete.",
        runtimeSummary.getIdentity(),
        workflowSummary.getIdentity());
    return Result.of(State.DONE);
  }
}
