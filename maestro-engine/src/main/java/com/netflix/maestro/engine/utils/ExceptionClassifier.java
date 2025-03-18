/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.engine.utils;

import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;

/** Utility class for further classifying exceptions. */
public final class ExceptionClassifier {
  private ExceptionClassifier() {}

  private static final String NOT_FOUND_MESSAGE = "has not been created yet";
  private static final String NO_ACTIVE_VERSION_MESSAGE =
      "Cannot find an active version for workflow";
  private static final String TIME_TRIGGER_DISABLED_MESSAGE =
      "Trigger type [TIME] is disabled for the workflow";
  private static final String SIGNAL_TRIGGER_DISABLED_MESSAGE =
      "Trigger type [SIGNAL] is disabled for the workflow";

  /**
   * Classify if it is a workflow not found exception vs others such as invalid URL, etc.
   *
   * @param exception Exception
   * @return true if workflow not found exception, false otherwise
   */
  public static boolean isWorkflowNotFoundException(MaestroNotFoundException exception) {
    return exception.getMessage() != null && exception.getMessage().contains(NOT_FOUND_MESSAGE);
  }

  /**
   * Classify if it is no active version for workflow case.
   *
   * @param exception Exception
   * @return true if no active version exception, false otherwise
   */
  public static boolean isNoActiveWorkflowVersionException(MaestroNotFoundException exception) {
    return exception.getMessage() != null
        && exception.getMessage().contains(NO_ACTIVE_VERSION_MESSAGE);
  }

  /**
   * Time trigger is disabled case.
   *
   * @param exception Exception
   * @return true if time trigger is disabled exception, false otherwise
   */
  public static boolean isTimeTriggerDisabledException(
      MaestroUnprocessableEntityException exception) {
    return exception.getMessage() != null
        && exception.getMessage().contains(TIME_TRIGGER_DISABLED_MESSAGE);
  }

  /**
   * Signal trigger is disabled case.
   *
   * @param exception Exception
   * @return true if signal trigger is disabled exception, false otherwise
   */
  public static boolean isSignalTriggerDisabledException(
      MaestroUnprocessableEntityException exception) {
    return exception.getMessage() != null
        && exception.getMessage().contains(SIGNAL_TRIGGER_DISABLED_MESSAGE);
  }
}
