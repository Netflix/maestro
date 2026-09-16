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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Phases of a step instance that can carry their own timeout. Each phase knows which runtime state
 * time its clock starts from and in which step statuses it applies.
 */
public enum TimeoutPhase {
  /** Whole step instance, from create time, in every status. */
  STEP(StepRuntimeState::getCreateTime, status -> true),
  /** Waiting for signals, from wait signal time, only in WAITING_FOR_SIGNALS status. */
  WAITING_FOR_SIGNALS(
      StepRuntimeState::getWaitSignalTime,
      status -> status == StepInstance.Status.WAITING_FOR_SIGNALS),
  /** Waiting for tag permits, from wait permit time, only in WAITING_FOR_PERMITS status. */
  WAITING_FOR_PERMITS(
      StepRuntimeState::getWaitPermitTime,
      status -> status == StepInstance.Status.WAITING_FOR_PERMITS),
  /** Running, from start time, in STARTING, RUNNING and FINISHING statuses. */
  RUNNING(
      StepRuntimeState::getStartTime,
      status ->
          status == StepInstance.Status.STARTING
              || status == StepInstance.Status.RUNNING
              || status == StepInstance.Status.FINISHING);

  private final Function<StepRuntimeState, Long> clockStart;
  private final Predicate<StepInstance.Status> applicableStatus;

  TimeoutPhase(
      Function<StepRuntimeState, Long> clockStart,
      Predicate<StepInstance.Status> applicableStatus) {
    this.clockStart = clockStart;
    this.applicableStatus = applicableStatus;
  }

  /** Returns the time this phase's clock started for the given state, or null if not started. */
  @Nullable
  public Long getClockStart(StepRuntimeState state) {
    return clockStart.apply(state);
  }

  /** Returns true if this phase's timeout applies while the step is in the given status. */
  public boolean appliesTo(StepInstance.Status status) {
    return applicableStatus.test(status);
  }

  /** Static creator. */
  @JsonCreator
  public static TimeoutPhase create(String phase) {
    return TimeoutPhase.valueOf(phase.toUpperCase(Locale.US));
  }
}
