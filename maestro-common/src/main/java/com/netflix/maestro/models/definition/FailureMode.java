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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;

/**
 * Supported step failure model. Note that IGNORE_FAILURE failure mode won't apply to
 * INTERNALLY_FAILED errors.
 */
public enum FailureMode {
  /**
   * If failed after all retries, the workflow instance run is immediately failed and all running
   * steps are stopped.
   */
  FAIL_IMMEDIATELY,
  /**
   * If failed after all retries, the system waits all running steps and its downstream steps to
   * enter a terminal state and then fail the workflow instance run.
   */
  FAIL_AFTER_RUNNING,
  /**
   * If failed after all retries, the system ignores the step failure and marks the step status to
   * be COMPLETED_WITH_ERROR and then proceeds to the next step.
   */
  IGNORE_FAILURE;

  /** Static creator. */
  @JsonCreator
  public static FailureMode create(String fm) {
    return FailureMode.valueOf(fm.toUpperCase(Locale.US));
  }
}
