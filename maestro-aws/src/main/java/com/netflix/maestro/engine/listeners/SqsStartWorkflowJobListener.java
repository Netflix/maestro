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

import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.processors.MaestroEventProcessor;
import com.netflix.maestro.engine.processors.SqsProcessorFinalizer;
import lombok.AllArgsConstructor;
import org.springframework.cloud.aws.messaging.listener.Acknowledgment;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.Visibility;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.messaging.handler.annotation.Header;

/** SQS Listener for StartWorkflowJob SQS queue. */
@AllArgsConstructor
public class SqsStartWorkflowJobListener {
  private final MaestroEventProcessor<StartWorkflowJobEvent> messageProcessor;
  private final SqsProcessorFinalizer sqsProcessorFinalizer;

  /** Listener configuration for SQS StartWorkflowJobEvent message. */
  @SqsListener(
      value = "${aws.sqs.start-workflow-job-queue-url}",
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
        0,
        receiveCount,
        messageProcessor,
        StartWorkflowJobEvent.class);
  }
}
