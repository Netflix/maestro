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
package com.netflix.maestro.extensions.handlers;

import com.netflix.maestro.engine.publisher.MaestroNotificationPublisher;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.extensions.provider.MaestroDataProvider;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.WorkflowInstance;
import lombok.extern.slf4j.Slf4j;

/**
 * A decorator around {@link MaestroNotificationPublisher} that intercepts {@link
 * StepInstanceStatusChangeEvent}s and processes them through the {@link ForeachFlatteningHandler}
 * before delegating to the original publisher.
 *
 * <p>In internal Maestro, foreach flattening events arrive via SQS. In OSS, we intercept them at
 * the notification publisher level since all components run in the same JVM.
 */
@Slf4j
public class ForeachNotificationInterceptor implements MaestroNotificationPublisher {
  private final MaestroNotificationPublisher delegate;
  private final MaestroDataProvider dataProvider;
  private final ForeachFlatteningHandler foreachFlatteningHandler;

  public ForeachNotificationInterceptor(
      MaestroNotificationPublisher delegate,
      MaestroDataProvider dataProvider,
      ForeachFlatteningHandler foreachFlatteningHandler) {
    this.delegate = delegate;
    this.dataProvider = dataProvider;
    this.foreachFlatteningHandler = foreachFlatteningHandler;
  }

  @Override
  public void send(MaestroEvent event) {
    if (event instanceof StepInstanceStatusChangeEvent stepEvent) {
      try {
        WorkflowInstance workflowInstance =
            dataProvider.getWorkflowInstance(
                stepEvent.getWorkflowId(),
                stepEvent.getWorkflowInstanceId(),
                stepEvent.getWorkflowRunId());
        var input =
            new StepEventHandlerInput(
                workflowInstance, stepEvent.getStepId(), stepEvent.getStepAttemptId());
        foreachFlatteningHandler.process(input);
      } catch (Exception e) {
        // Log but do not block the notification pipeline
        LOG.warn(
            "Failed to process foreach flattening for workflowId={}, instanceId={}, runId={}, stepId={}, attemptId={}",
            stepEvent.getWorkflowId(),
            stepEvent.getWorkflowInstanceId(),
            stepEvent.getWorkflowRunId(),
            stepEvent.getStepId(),
            stepEvent.getStepAttemptId(),
            e);
      }
    }
    // Always delegate to the original publisher
    delegate.send(event);
  }
}
