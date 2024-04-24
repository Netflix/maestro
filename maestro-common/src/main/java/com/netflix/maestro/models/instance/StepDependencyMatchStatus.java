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
package com.netflix.maestro.models.instance;

import java.util.Locale;

/** Various step dependency match status. */
public enum StepDependencyMatchStatus {
  /** MATCHED status indicating that the signal dependency conditions have been fulfilled. */
  MATCHED("MATCHED"),
  /**
   * PENDING status indicating that the step is waiting for signal dependency conditions to be
   * fulfilled.
   */
  PENDING("PENDING"),
  /** SKIPPED status indicating that conditions are skipped by a BYPASS_STEP_DEPENDENCIES action. */
  SKIPPED("SKIPPED");

  /**
   * getter for status.
   *
   * @return status string
   */
  public String getStatus() {
    return this.status;
  }

  private final String status;

  StepDependencyMatchStatus(String status) {
    this.status = status;
  }

  /**
   * returns @{@link StepDependencyMatchStatus} for a given status label.
   *
   * @param status status
   * @return matching status
   */
  public static StepDependencyMatchStatus create(String status) {
    return StepDependencyMatchStatus.valueOf(status.toUpperCase(Locale.US));
  }
}
