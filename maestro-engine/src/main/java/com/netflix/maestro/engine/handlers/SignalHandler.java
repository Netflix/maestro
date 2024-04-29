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
package com.netflix.maestro.engine.handlers;

import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.List;
import java.util.Map;

/**
 * Signal handler interface to implement signal dependencies and output signal features. During the
 * maestro task execution, {@link #signalsReady} method is called to check if the signal
 * dependencies are matched. At the end of the task execution, {@link #sendOutputSignals} method is
 * called to emit signals, which can trigger other workflows or satisfy other step dependencies.
 */
public interface SignalHandler {
  /**
   * Sends output signal.
   *
   * @param stepRuntimeSummary the step runtime summary
   * @return true if the signal is sent successfully, false otherwise
   */
  boolean sendOutputSignals(WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary);

  /**
   * Checks the signal status of a StepRuntimeSummary. This method also updates the {@link
   * StepRuntimeSummary} if there is an update for any signals.
   *
   * @param workflowSummary the workflow summary
   * @param stepRuntimeSummary the step runtime summary
   * @return true if the signals are ready, false otherwise
   */
  boolean signalsReady(WorkflowSummary workflowSummary, StepRuntimeSummary stepRuntimeSummary);

  /**
   * Get the parameters from signal dependencies.
   *
   * @param runtimeSummary the step runtime summary
   * @return the map of signal parameters
   */
  Map<String, List<Map<String, Parameter>>> getDependenciesParams(
      StepRuntimeSummary runtimeSummary);
}
