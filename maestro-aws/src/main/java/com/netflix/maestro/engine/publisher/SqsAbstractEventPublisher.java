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
package com.netflix.maestro.engine.publisher;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.jobevents.MaestroJobEvent;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.error.Details;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class SqsAbstractEventPublisher implements MaestroJobEventPublisher {
  private final AmazonSQS amazonSqs;
  private final ObjectMapper objectMapper;
  private final MaestroMetrics metrics;

  public SqsAbstractEventPublisher(
      AmazonSQS amazonSqs, ObjectMapper objectMapper, MaestroMetrics metricRepo) {
    this.amazonSqs = amazonSqs;
    this.objectMapper = objectMapper;
    this.metrics = metricRepo;
  }

  @Override
  public Optional<Details> publish(MaestroJobEvent jobEvent, long invisibleMs) {
    try {
      String strRequest = objectMapper.writeValueAsString(jobEvent);
      int delaySec = (int) TimeUnit.MILLISECONDS.toSeconds(invisibleMs);
      SendMessageRequest sendMsgRequest =
          new SendMessageRequest()
              .withQueueUrl(getQueueUrlToPublish(jobEvent))
              .withMessageBody(strRequest)
              .withDelaySeconds(delaySec);
      amazonSqs.sendMessage(convertToFifoIfApplicable(jobEvent, sendMsgRequest));
      metrics.counter(
          AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_SUCCESS_METRIC,
          getClass(),
          AwsMetricConstants.JOB_TYPE_TAG,
          jobEvent.getClass().getSimpleName());
      return Optional.empty();
    } catch (Exception e) {
      metrics.counter(
          AwsMetricConstants.SQS_JOB_EVENT_PUBLISH_FAILURE_METRIC,
          getClass(),
          MetricConstants.TYPE_TAG,
          e.getClass().getSimpleName(),
          AwsMetricConstants.JOB_TYPE_TAG,
          jobEvent.getClass().getSimpleName());
      LOG.error(
          "Error when enqueuing [{}] with exception: ", jobEvent.getClass().getSimpleName(), e);
      return detailsToPublishOnException(e);
    }
  }

  private SendMessageRequest convertToFifoIfApplicable(
      MaestroJobEvent event, SendMessageRequest sendMessageRequest) {
    Optional<String> uniqueKeyOpt =
        uniqueKeyForFifoDeduplication(event, sendMessageRequest.getMessageBody());
    if (uniqueKeyOpt.isEmpty()) {
      return sendMessageRequest;
    }
    String uniqueKey = uniqueKeyOpt.get();
    // Fifo request conversion
    return sendMessageRequest.withMessageGroupId(uniqueKey).withMessageDeduplicationId(uniqueKey);
  }

  /**
   * Specify what Optional<Details> to return in case of encountering an exception during
   * publishing.
   */
  protected abstract Optional<Details> detailsToPublishOnException(Exception ex);

  /** Return the SQS queue url for a given event. */
  protected abstract String getQueueUrlToPublish(MaestroJobEvent event);

  /**
   * Indicates how to extract the unique key used for deduplication purposes in FIFO. If it is not
   * present then assuming it is an event published to a non-FIFO standard SQS queue.
   *
   * @param event the event from which we derived the deduplication key
   * @param messageBody the deserialized message body of the event
   */
  protected Optional<String> uniqueKeyForFifoDeduplication(
      MaestroJobEvent event, String messageBody) {
    return Optional.empty();
  }
}
