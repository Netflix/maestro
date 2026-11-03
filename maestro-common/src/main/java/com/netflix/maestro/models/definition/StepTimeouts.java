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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.validations.TimeoutConstraint;
import java.util.EnumMap;
import java.util.Map;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Per-phase step timeouts, see {@link TimeoutPhase} for the clock and statuses of each phase. Every
 * value is optional. A phase without a value is unbounded, except a phase carrying a system
 * default.
 */
@Builder(toBuilder = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"step", "waiting_for_signals", "waiting_for_permits", "running"},
    alphabetic = true)
@JsonDeserialize(builder = StepTimeouts.StepTimeoutsBuilder.class)
@Getter
@EqualsAndHashCode
public class StepTimeouts {
  /** Timeout for the whole step instance, in seconds or as a duration string. */
  @TimeoutConstraint private final ParsableLong step;

  /** Timeout while waiting for signals, in seconds or as a duration string. */
  @TimeoutConstraint private final ParsableLong waitingForSignals;

  /** Timeout while waiting for tag permits, in seconds or as a duration string. */
  @TimeoutConstraint private final ParsableLong waitingForPermits;

  /** Timeout while running, in seconds or as a duration string. */
  @TimeoutConstraint private final ParsableLong running;

  /** Returns the set phases keyed by phase, in phase order. */
  public Map<TimeoutPhase, ParsableLong> toMap() {
    Map<TimeoutPhase, ParsableLong> phases = new EnumMap<>(TimeoutPhase.class);
    if (step != null) {
      phases.put(TimeoutPhase.STEP, step);
    }
    if (waitingForSignals != null) {
      phases.put(TimeoutPhase.WAITING_FOR_SIGNALS, waitingForSignals);
    }
    if (waitingForPermits != null) {
      phases.put(TimeoutPhase.WAITING_FOR_PERMITS, waitingForPermits);
    }
    if (running != null) {
      phases.put(TimeoutPhase.RUNNING, running);
    }
    return phases;
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class StepTimeoutsBuilder {}
}
