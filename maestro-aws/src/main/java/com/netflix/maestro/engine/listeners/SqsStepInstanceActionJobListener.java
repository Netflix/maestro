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
package com.netflix.maestro.engine.listeners;

import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.processors.MaestroEventProcessor;
import com.netflix.maestro.engine.processors.SqsProcessorFinalizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.aws.messaging.listener.Acknowledgment;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.Visibility;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.handler.annotation.Header;

/** SQS Listener for StepInstanceWakeUpEvent SQS queue. */
@Slf4j
@RequiredArgsConstructor
public class SqsStepInstanceActionJobListener {
  /**
   * Invisible time for reprocessing a step instance wake up event. It is usually used for re-check
   * if a non-leaf step has reached to its desired state.
   */
  private static final int REPROCESS_STEP_INSTANCE_WAKE_UP_EVENT_DELAY_IN_SECONDS = 6;

  /** The max number of retries to process a step instance wake up event. */
  private static final int STEP_INSTANCE_WAKE_UP_EVENT_RETRY_LIMIT = 6;

  private final MaestroEventProcessor<StepInstanceWakeUpEvent> messageProcessor;
  private final SqsProcessorFinalizer sqsProcessorFinalizer;

  private final MaestroEventProcessor<StepInstanceWakeUpEvent> fallbackProcessorOverRetryLimit =
      messageSupplier -> {
        StepInstanceWakeUpEvent event = messageSupplier.get();
        LOG.warn(
            "this step wake up event: {} is over the retry limit, will discard.",
            event.getMessageKey());
      };

  @SqsListener(
      value = "${aws.sqs.step-wake-up-job-queue-url}",
      deletionPolicy = SqsMessageDeletionPolicy.NEVER)
  public void process(
      String payload,
      Acknowledgment acknowledgment,
      Visibility visibility,
      @Header("ApproximateReceiveCount") int receiveCount) {
    sqsProcessorFinalizer.process(
        payload,
        acknowledgment::acknowledge,
        visibility::extend,
        REPROCESS_STEP_INSTANCE_WAKE_UP_EVENT_DELAY_IN_SECONDS,
        receiveCount,
        receiveCount > STEP_INSTANCE_WAKE_UP_EVENT_RETRY_LIMIT
            ? fallbackProcessorOverRetryLimit
            : messageProcessor,
        StepInstanceWakeUpEvent.class);
  }
}
