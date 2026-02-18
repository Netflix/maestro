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

import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.extensions.handlers.ForeachFlatteningHandler;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.MaestroEvent.Type;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;

/**
 * Processor for {@link MaestroEvent}s received from SQS. Handles {@link
 * StepInstanceStatusChangeEvent} by preprocessing and delegating to {@link
 * ForeachFlatteningHandler}.
 *
 * <p>Mirrors internal's {@code MaestroEventProcessor} but without targeted-at-group, Trino, and
 * subscription handling which are not yet ported to OSS.
 */
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class MaestroEventProcessor {
  private static final String TYPE_TAG = "type";
  private static final String METRIC_PROCESSOR_FAILURE = "maestroevent.processor.failure";
  private static final String METRIC_PROCESSOR_DURATION = "maestroevent.processor.duration";

  private final StepEventPreprocessor stepEventPreprocessor;
  private final ForeachFlatteningHandler foreachFlatteningHandler;
  private final MaestroMetrics metrics;

  public MaestroEventProcessor(
      StepEventPreprocessor stepEventPreprocessor,
      ForeachFlatteningHandler foreachFlatteningHandler,
      MaestroMetrics metrics) {
    this.stepEventPreprocessor = stepEventPreprocessor;
    this.foreachFlatteningHandler = foreachFlatteningHandler;
    this.metrics = metrics;
  }

  /** Process a MaestroEvent. Only STEP_INSTANCE_STATUS_CHANGE_EVENT is handled. */
  public void process(MaestroEvent maestroEvent) {
    long start = Instant.now().toEpochMilli();
    Type type = null;
    try {
      type = maestroEvent.getType();
      if (type == Type.STEP_INSTANCE_STATUS_CHANGE_EVENT) {
        StepEventHandlerInput stepEventHandlerInput =
            stepEventPreprocessor.preprocess((StepInstanceStatusChangeEvent) maestroEvent);
        if (stepEventHandlerInput != null) {
          foreachFlatteningHandler.process(stepEventHandlerInput);
        }
      }
    } catch (Exception ex) {
      LOG.error("error during processing MaestroEvent {}", maestroEvent, ex);
      metrics.counter(METRIC_PROCESSOR_FAILURE, getClass());
      throw new MaestroRetryableError(ex, "unknown error during maestroEvent processing");
    } finally {
      String metricTag = getMaestroEventMetricTag(type);
      metrics.timer(
          METRIC_PROCESSOR_DURATION,
          Instant.now().toEpochMilli() - start,
          getClass(),
          TYPE_TAG,
          metricTag);
    }
  }

  private static String getMaestroEventMetricTag(final Type type) {
    if (type == Type.STEP_INSTANCE_STATUS_CHANGE_EVENT) {
      return "stepupdate";
    } else if (type == Type.WORKFLOW_INSTANCE_STATUS_CHANGE_EVENT) {
      return "workflowupdate";
    } else {
      return "other";
    }
  }
}
