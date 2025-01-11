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
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Maestro Sleep step runtime.
 *
 * <p>It guarantees sleeping at least `sleep_seconds` seconds.
 */
@Slf4j
@NoArgsConstructor
public class SleepStepRuntime implements StepRuntime {
  private static final String SLEEP_TIME_FIELD = "sleep_seconds";
  private static final int DEFAULT_SLEEP_TIME_IN_MILLIS = 3000;

  @Override
  public Result execute(
      WorkflowSummary workflowSummary, Step step, StepRuntimeSummary runtimeSummary) {
    long elapsedTime = System.currentTimeMillis() - runtimeSummary.getRuntimeState().getStartTime();

    Map<String, Parameter> stepParams = runtimeSummary.getParams();
    long sleepTime = DEFAULT_SLEEP_TIME_IN_MILLIS;
    if (stepParams.containsKey(SLEEP_TIME_FIELD)) {
      sleepTime = TimeUnit.SECONDS.toMillis(stepParams.get(SLEEP_TIME_FIELD).asLong());
    }

    LOG.debug(
        "Running Sleep step with sleep time [{}] millis, already elapsed {} millis, "
            + "for workflow instance {}'s step instance {}",
        sleepTime,
        elapsedTime,
        workflowSummary.getIdentity(),
        runtimeSummary.getIdentity());

    if (elapsedTime > sleepTime) {
      return Result.of(State.DONE);
    } else {
      return Result.of(State.USER_ERROR);
    }
  }
}
