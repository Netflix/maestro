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
package com.netflix.maestro.engine.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.exceptions.MaestroInternalError;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This class handles all boilerplate logics related to SQS listeners, e.g. acknowledgement,
 * exception handling, and exporting failure/success metrics.
 */
@Slf4j
@AllArgsConstructor
public class SqsProcessorFinalizer {
  /** threshold for high number of retries. */
  private static final int RECEIVE_COUNT_THRESHOLD_FOR_ADDITIONAL_MONITORING = 100;

  private final ObjectMapper objectMapper;
  private final MaestroMetricRepo metrics;

  /**
   * If it evaluates true for an exception, the currently processing message will be deleted from
   * queue, otherwise, it will be retried later with a visibility timeout provided in {@link
   * SqsProcessorFinalizer#process(String, Runnable, Consumer, int, int, MaestroEventProcessor,
   * Class)}.
   */
  private final ExceptionEventDeletionPolicy exceptionEventDeletionPolicy;

  /**
   * Handles boilerplate related to SQS listeners, e.g. acknowledgement, exception handling, and
   * exporting failure/success metrics after calling the underlying message processor.
   *
   * @param payload the payload used for deserializing and sending to underlying processor.
   * @param acknowledgement refers to the SQS callback to acknowledge and delete message from queue.
   * @param setVisibility callback to change the visibility of the messages in SQS.
   * @param visibilityTimeoutInSecs the visibility timeout in seconds to use to process exceptions
   *     that do not apply to exceptionQueueDeletionPolicy
   * @param processor the underlying {@link MaestroEventProcessor} handling the payload.
   * @param clazz the runtime class to be used for the deserialization of the payload.
   * @param <T> the runtime type parameter of the class to be used for deserialization.
   */
  public <T extends MaestroJobEvent> void process(
      String payload,
      Runnable acknowledgement,
      Consumer<Integer> setVisibility,
      int visibilityTimeoutInSecs,
      int receiveCount,
      MaestroEventProcessor<T> processor,
      Class<T> clazz) {
    try {
      long start = metrics.clock().monotonicTime();
      processor.process(
          () -> {
            try {
              T message = objectMapper.readValue(payload, clazz);
              LOG.debug(
                  "Received a [{}] from SQS and processing it: [{}]",
                  clazz.getSimpleName(),
                  message);
              return message;
            } catch (IOException ex) {
              throw new MaestroInternalError(ex, "exception during json parsing");
            }
          });
      long latencyInMillis = TimeUnit.NANOSECONDS.toMillis(metrics.clock().monotonicTime() - start);
      metrics.timer(
          AwsMetricConstants.SQS_PROCESSOR_LATENCY_METRIC,
          latencyInMillis,
          getClass(),
          AwsMetricConstants.JOB_TYPE_TAG,
          clazz.getSimpleName());
      acknowledgement.run();
      metrics.counter(
          AwsMetricConstants.SQS_JOB_EVENT_LISTENER_SUCCESS_METRIC,
          getClass(),
          AwsMetricConstants.JOB_TYPE_TAG,
          clazz.getSimpleName());
    } catch (Exception ex) {
      if (exceptionEventDeletionPolicy.getPolicy().apply(ex)) {
        metrics.counter(
            AwsMetricConstants.SQS_JOB_EVENT_LISTENER_FAILURE_METRIC,
            getClass(),
            AwsMetricConstants.JOB_TYPE_TAG,
            clazz.getSimpleName(),
            MetricConstants.TYPE_TAG,
            ex.getClass().getSimpleName());
        LOG.warn(
            "Deleting exception from queue. Exception [{}] when processing payload [{}]",
            ex.getClass().getSimpleName(),
            payload,
            ex);
        acknowledgement.run();
      } else {
        if (receiveCount >= RECEIVE_COUNT_THRESHOLD_FOR_ADDITIONAL_MONITORING) {
          LOG.warn(
              "SQS payload: [{}] has been retrying [{}] times, check if there are any problems.",
              payload,
              receiveCount);
          metrics.counter(
              AwsMetricConstants.SQS_EVENT_HIGH_NUMBER_OF_RETRIES,
              getClass(),
              AwsMetricConstants.JOB_TYPE_TAG,
              clazz.getSimpleName());
        }
        LOG.warn(
            "Exception [{}] when processing class type [{}], to be retried later",
            ex.getClass().getSimpleName(),
            clazz.getSimpleName(),
            ex);
        setVisibility.accept(visibilityTimeoutInSecs);
      }
    }
  }
}
