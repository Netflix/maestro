/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.engine.properties;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.TimeoutPhase;
import java.util.EnumMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * Step timeout properties. Please check {@link com.netflix.maestro.engine.tasks.MaestroTask} about
 * how they are used.
 */
@Getter
@Setter
public class StepTimeoutProperties {
  /** Step timeout applied when a step definition does not set one; null means unbounded. */
  @Nullable private Long defaultStepTimeoutInMillis;

  /** Waiting for signals timeout applied when a step definition does not set one. */
  @Nullable private Long defaultWaitingForSignalsTimeoutInMillis;

  /** Waiting for permits timeout applied when a step definition does not set one. */
  @Nullable private Long defaultWaitingForPermitsTimeoutInMillis;

  /** Running timeout applied when a step definition does not set one. */
  @Nullable private Long defaultRunningTimeoutInMillis = Defaults.DEFAULT_TIME_OUT_LIMIT_IN_MILLIS;

  /** Phase that the single step {@code timeout} field applies to. */
  private TimeoutPhase defaultTimeoutPhase = TimeoutPhase.RUNNING;

  /** Returns the default timeout in millis per phase. A phase without a default is unbounded. */
  public Map<TimeoutPhase, Long> getDefaultTimeoutsInMillis() {
    Map<TimeoutPhase, Long> defaults = new EnumMap<>(TimeoutPhase.class);
    putIfSet(defaults, TimeoutPhase.STEP, defaultStepTimeoutInMillis);
    putIfSet(defaults, TimeoutPhase.WAITING_FOR_SIGNALS, defaultWaitingForSignalsTimeoutInMillis);
    putIfSet(defaults, TimeoutPhase.WAITING_FOR_PERMITS, defaultWaitingForPermitsTimeoutInMillis);
    putIfSet(defaults, TimeoutPhase.RUNNING, defaultRunningTimeoutInMillis);
    return defaults;
  }

  private static void putIfSet(
      Map<TimeoutPhase, Long> defaults, TimeoutPhase phase, @Nullable Long timeoutInMillis) {
    if (timeoutInMillis != null) {
      defaults.put(phase, timeoutInMillis);
    }
  }
}
