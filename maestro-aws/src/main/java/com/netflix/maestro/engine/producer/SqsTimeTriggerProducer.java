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
package com.netflix.maestro.engine.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.producer.TimeTriggerProducer;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Producer for TimeTriggerExecution messages. */
@AllArgsConstructor
@Slf4j
public class SqsTimeTriggerProducer implements TimeTriggerProducer {
  private final SqsTemplate amazonSqs;
  private final ObjectMapper objectMapper;
  private final SqsProperties props;
  private final MaestroMetrics metrics;

  /** Push TimeTriggerExecution message. */
  public void push(TimeTriggerExecution execution, int delaySecs) {
    try {
      LOG.debug("Publishing time trigger execution: [{}] with delay [{}]", execution, delaySecs);
      String strRequest = objectMapper.writeValueAsString(execution);
      amazonSqs.send(
          sqsSendOptions ->
              sqsSendOptions
                  .queue(props.getTimeTriggerExecutionQueueUrl())
                  .payload(strRequest)
                  .delaySeconds(delaySecs));
      metrics.counter(
          AwsMetricConstants.SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC,
          getClass(),
          "withDelay",
          String.valueOf(delaySecs > 0));
    } catch (Exception e) {
      metrics.counter(AwsMetricConstants.SQS_TIME_TRIGGER_PUBLISH_FAILURE_METRIC, getClass());
      LOG.error(
          "Error when enqueuing time trigger execution for [{}] with exception: ", execution, e);
      throw new RuntimeException(e);
    }
  }
}
