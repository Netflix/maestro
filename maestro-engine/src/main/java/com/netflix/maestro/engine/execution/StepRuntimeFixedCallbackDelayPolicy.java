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
package com.netflix.maestro.engine.execution;

import com.netflix.maestro.engine.properties.CallbackDelayConfig;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.utils.Checks;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import lombok.AllArgsConstructor;

/** Fixed callback delay policy implementation. */
@AllArgsConstructor
public class StepRuntimeFixedCallbackDelayPolicy implements StepRuntimeCallbackDelayPolicy {
  private static final String DEFAULT_STEP_TYPE_KEY = "default";

  private final CallbackDelayConfig callbackDelayConfigStepInstanceStatusMap;

  /**
   * Get fixed callback for the stepRuntimeSummary.
   *
   * <p>Obtain the call back delay config map for the step instance status.
   *
   * <p>From the status map get the callback delay setting for a particular step.
   *
   * @param runtimeSummary stepRuntime summary.
   * @return callback delay in secs or null if not configured.
   */
  @Override
  public Long getCallBackDelayInSecs(StepRuntimeSummary runtimeSummary) {
    if (runtimeSummary != null) {
      StepInstance.Status stepInstanceStatus = runtimeSummary.getRuntimeState().getStatus();
      Map<String, Long> callbackDelayConfigForStepInstanceStatus =
          callbackDelayConfigStepInstanceStatusMap
              .getFixedCallbackDelayInSecs()
              .get(stepInstanceStatus);
      if (callbackDelayConfigForStepInstanceStatus == null) {
        return null;
      } else {
        Map<String, Long> fixedCallbackDelayConfig =
            new LinkedHashMap<>(callbackDelayConfigForStepInstanceStatus);
        return getCallbackDelayForStepType(
            fixedCallbackDelayConfig, runtimeSummary, stepInstanceStatus);
      }
    }
    return null;
  }

  @Override
  public CallbackDelayPolicyType getType() {
    return CallbackDelayPolicyType.FIXED_CALLBACK_DELAY;
  }

  private Long getCallbackDelayForStepType(
      Map<String, Long> fixedCallbackDelayConfigForStepInstanceStatus,
      StepRuntimeSummary runtimeSummary,
      StepInstance.Status stepInstanceStatus) {
    String stepType = runtimeSummary.getType().getType().toLowerCase(Locale.US);
    Long callbackDelay = fixedCallbackDelayConfigForStepInstanceStatus.get(stepType);
    if (callbackDelay == null) {
      Checks.checkTrue(
          fixedCallbackDelayConfigForStepInstanceStatus.containsKey(DEFAULT_STEP_TYPE_KEY),
          "StepRuntime callback delay config for "
              + stepInstanceStatus
              + " status must contain default step type setting");
      return fixedCallbackDelayConfigForStepInstanceStatus.get(DEFAULT_STEP_TYPE_KEY);
    } else {
      return callbackDelay;
    }
  }
}
