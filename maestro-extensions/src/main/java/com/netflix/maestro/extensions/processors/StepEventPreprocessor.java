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
package com.netflix.maestro.extensions.processors;

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.extensions.provider.MaestroClient;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.WorkflowInstance;
import lombok.extern.slf4j.Slf4j;

/**
 * Preprocessor that fetches the {@link WorkflowInstance} from maestro-server for a given {@link
 * StepInstanceStatusChangeEvent} and creates a {@link StepEventHandlerInput}.
 */
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class StepEventPreprocessor {
  private static final String METRIC_INSTANCE_NOT_FOUND =
      "maestroevent.preprocessor.instance.not.found";

  private final MaestroClient maestroClient;
  private final MaestroMetrics metrics;

  public StepEventPreprocessor(MaestroClient maestroClient, MaestroMetrics metrics) {
    this.maestroClient = maestroClient;
    this.metrics = metrics;
  }

  /** Preprocess a step event by fetching the workflow instance from maestro-server. */
  @Nullable
  public StepEventHandlerInput preprocess(StepInstanceStatusChangeEvent stepEvent) {
    StepEventHandlerInput stepEventHandlerInput = null;
    try {
      WorkflowInstance workflowInstance =
          maestroClient.getWorkflowInstance(
              stepEvent.getWorkflowId(),
              stepEvent.getWorkflowInstanceId(),
              stepEvent.getWorkflowRunId());
      stepEventHandlerInput =
          new StepEventHandlerInput(
              workflowInstance, stepEvent.getStepId(), stepEvent.getStepAttemptId());
    } catch (MaestroNotFoundException e) {
      LOG.warn(
          "Didn't find workflowId={}, instanceId={}, runId={}, stepId={}, attemptId={} "
              + "in maestro, either deleted or not created",
          stepEvent.getWorkflowId(),
          stepEvent.getWorkflowInstanceId(),
          stepEvent.getWorkflowRunId(),
          stepEvent.getStepId(),
          stepEvent.getStepAttemptId());
      metrics.counter(METRIC_INSTANCE_NOT_FOUND, getClass());
    }
    return stepEventHandlerInput;
  }
}
