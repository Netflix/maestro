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
package com.netflix.maestro.engine.properties;

import lombok.Getter;
import lombok.Setter;

/** SQS configuration properties. */
@Getter
@Setter
public class SqsProperties {
  /**
   * Max pool size is based on SQS implementation in {@link
   * org.springframework.cloud.aws.messaging.listener.SimpleMessageListenerContainer} where we have
   * one spinning thread per-queue and worker threads equal to message batch size of the queue.
   * Since we have 8 queues in maestro we do 8 * ({@link
   * SqsProperties#DEFAULT_LISTENER_MAX_NUMBER_OF_MESSAGES} + 1) = 25 core pool size.
   */
  private static final int DEFAULT_MAX_POOL_SIZE = 25;

  private static final int DEFAULT_CORE_POOL_SIZE = 10;
  private static final int DEFAULT_LISTENER_CAPACITY = 0;
  private static final int DEFAULT_LISTENER_MAX_NUMBER_OF_MESSAGES = 3;
  private static final int DEFAULT_LISTENER_WAIT_TIMEOUT_IN_SECS = 1;

  private String startWorkflowJobQueueUrl;
  private String runWorkflowInstancesJobQueueUrl;
  private String terminateInstancesJobQueueUrl;
  private String terminateThenRunInstanceJobQueueUrl;
  private String publishJobQueueUrl;
  private String deleteWorkflowJobQueueUrl;
  private String stepWakeUpJobQueueUrl;
  private String timeTriggerExecutionQueueUrl;
  private String signalInstanceQueueUrl;
  private String signalTriggerMatchQueueUrl;
  private String signalTriggerExecutionQueueUrl;

  /** Core pool size of the thread pool executor used by SQS message listeners. */
  private int listenerCorePoolSize = DEFAULT_CORE_POOL_SIZE;

  /** Max pool size of the thread pool executor used by SQS message listeners. */
  private int listenerMaxPoolSize = DEFAULT_MAX_POOL_SIZE;

  /** Queue capacity of the thread pool executor used by SQS message listeners. */
  private int listenerQueueCapacity = DEFAULT_LISTENER_CAPACITY;

  /**
   * Max number/batch of messages read at a time by one poll of SQS queue's message listener. This
   * is limited to 10 per poll by SQS.
   */
  private int listenerMaxNumberOfMessages = DEFAULT_LISTENER_MAX_NUMBER_OF_MESSAGES;

  /**
   * The duration (in seconds) for which the call waits for a message to arrive in the queue before
   * returning.
   */
  private int listenerWaitTimeoutInSecs = DEFAULT_LISTENER_WAIT_TIMEOUT_IN_SECS;
}
