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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sqs.AmazonSQS;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.properties.SqsProperties;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SqsTimeTriggerProducerTest extends MaestroBaseTest {
  private AmazonSQS amazonSqs;
  private MaestroMetricRepo metricRepo;

  private SqsTimeTriggerProducer timeTriggerProducer;
  private TimeTriggerExecution execution;

  @Before
  public void setup() {
    amazonSqs = mock(AmazonSQS.class);
    SqsProperties sqsProperties = new SqsProperties();
    sqsProperties.setTimeTriggerExecutionQueueUrl("time-trigger-execution-queue-url");
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    timeTriggerProducer = new SqsTimeTriggerProducer(amazonSqs, MAPPER, sqsProperties, metricRepo);
    execution =
        TimeTriggerExecution.builder()
            .workflowId("test-wf-id")
            .workflowVersion("active")
            .workflowTriggerUuid("uuid")
            .timeTriggersWithWatermarks(
                List.of(
                    TimeTriggerWithWatermark.builder()
                        .timeTrigger(new CronTimeTrigger())
                        .lastTriggerTimestamp(123456L)
                        .build()))
            .build();
  }

  @Test
  public void testPushWithNoDelay() {
    timeTriggerProducer.push(execution, 0);
    verify(amazonSqs, times(1)).sendMessage(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC,
                SqsTimeTriggerProducer.class,
                "withDelay",
                "false")
            .count());
  }

  @Test
  public void testPushWithNegativeDelay() {
    timeTriggerProducer.push(execution, -100);
    verify(amazonSqs, times(1)).sendMessage(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC,
                SqsTimeTriggerProducer.class,
                "withDelay",
                "false")
            .count());
  }

  @Test
  public void testPushWithDelay() {
    timeTriggerProducer.push(execution, 100);
    verify(amazonSqs, times(1)).sendMessage(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_TIME_TRIGGER_PUBLISH_SUCCESS_METRIC,
                SqsTimeTriggerProducer.class,
                "withDelay",
                "true")
            .count());
  }

  @Test
  public void testPushWithError() {
    when(amazonSqs.sendMessage(any())).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Should throw the error",
        RuntimeException.class,
        "java.lang.RuntimeException: test",
        () -> timeTriggerProducer.push(execution, 0));
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_TIME_TRIGGER_PUBLISH_FAILURE_METRIC,
                SqsTimeTriggerProducer.class)
            .count());
  }
}
