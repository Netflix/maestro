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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.jobevents.DeleteWorkflowJobEvent;
import com.netflix.maestro.engine.metrics.AwsMetricConstants;
import com.netflix.maestro.engine.metrics.MaestroMetricRepo;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.spectator.api.DefaultRegistry;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import lombok.Getter;
import org.junit.Before;
import org.junit.Test;

public class SqsProcessorFinalizerTest extends MaestroBaseTest {
  private Runnable acknowledgement;
  private Consumer<Integer> setVisibility;

  private TestableMessageProcessorTest processor;
  private SqsProcessorFinalizer sqsProcessorFinalizer;
  private MaestroMetricRepo metricRepo;

  private static final String PAYLOAD =
      "{\"workflow_id\": \"deserialized_payload\", \"type\": \"DELETE_WORKFLOW_JOB_EVENT\"}";
  private static final DeleteWorkflowJobEvent DESERIALIZED_PAYLOAD =
      DeleteWorkflowJobEvent.create("deserialized_payload", 1, null);
  private static final Class<DeleteWorkflowJobEvent> CLAZZ = DeleteWorkflowJobEvent.class;
  private static final ExceptionEventDeletionPolicy EXCEPTION_QUEUE_DELETION_POLICY =
      ExceptionEventDeletionPolicy.DELETE_IF_NOT_MAESTRO_RETRYABLE_ERROR;
  private static final int RECEIVE_COUNT = 10;
  private static final int HIGH_RECEIVE_COUNT = 100;

  @Before
  public void setup() {
    acknowledgement = mock(Runnable.class);
    setVisibility = mock(Consumer.class);

    processor = new TestableMessageProcessorTest();
    metricRepo = new MaestroMetricRepo(new DefaultRegistry());
    sqsProcessorFinalizer =
        new SqsProcessorFinalizer(MAPPER, metricRepo, EXCEPTION_QUEUE_DELETION_POLICY);
  }

  @Test
  public void testSqsProcessorFinalizerNoException() {
    sqsProcessorFinalizer.process(
        PAYLOAD, acknowledgement, setVisibility, 2, RECEIVE_COUNT, processor, CLAZZ);

    assertEquals(DESERIALIZED_PAYLOAD.getWorkflowId(), processor.getInputUsedForProcessing());
    verify(acknowledgement, times(1)).run();
    verify(setVisibility, never()).accept(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_LISTENER_SUCCESS_METRIC,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
    assertEquals(
        0,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_EVENT_HIGH_NUMBER_OF_RETRIES,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
  }

  @Test
  public void testSqsProcessorFinalizerQueueDeletionExceptionSignalService() {
    sqsProcessorFinalizer.process(
        "deserialized_payload", acknowledgement, setVisibility, 2, RECEIVE_COUNT, processor, CLAZZ);

    assertNull(processor.getInputUsedForProcessing());
    verify(acknowledgement, times(1)).run();
    verify(setVisibility, never()).accept(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_LISTENER_FAILURE_METRIC,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent",
                MetricConstants.TYPE_TAG,
                "MaestroInternalError")
            .count());
    assertEquals(
        0,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_EVENT_HIGH_NUMBER_OF_RETRIES,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
  }

  @Test
  public void testSqsProcessorFinalizerQueueDeletionExceptionMaestroService() {
    SqsProcessorFinalizer finalizer =
        new SqsProcessorFinalizer(
            MAPPER, metricRepo, ExceptionEventDeletionPolicy.DELETE_IF_MAESTRO_INTERNAL_ERROR);
    finalizer.process(
        "deserialized_payload", acknowledgement, setVisibility, 2, RECEIVE_COUNT, processor, CLAZZ);

    assertNull(processor.getInputUsedForProcessing());
    verify(acknowledgement, times(1)).run();
    verify(setVisibility, never()).accept(any());
    assertEquals(
        1,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_LISTENER_FAILURE_METRIC,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent",
                MetricConstants.TYPE_TAG,
                "MaestroInternalError")
            .count());
    assertEquals(
        0,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_EVENT_HIGH_NUMBER_OF_RETRIES,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
  }

  @Test
  public void testSqsProcessorFinalizerVisibilityTriggerException() throws JsonProcessingException {
    sqsProcessorFinalizer.process(
        PAYLOAD,
        acknowledgement,
        setVisibility,
        2,
        RECEIVE_COUNT,
        new ThrowingMessageProcessorTest(),
        CLAZZ);

    verify(acknowledgement, never()).run();
    verify(setVisibility, times(1)).accept(2);
    assertEquals(
        0,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_LISTENER_SUCCESS_METRIC,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
    assertEquals(
        0,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_JOB_EVENT_LISTENER_FAILURE_METRIC,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent",
                MetricConstants.TYPE_TAG,
                "MaestroRetryableError")
            .count());
    assertEquals(
        0,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_EVENT_HIGH_NUMBER_OF_RETRIES,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
  }

  @Test
  public void testSqsProcessorFinalizerProcessingExceptionHighRetries() {
    int numMessages = 10;
    IntStream.rangeClosed(1, numMessages)
        .forEach(
            num ->
                sqsProcessorFinalizer.process(
                    PAYLOAD,
                    acknowledgement,
                    setVisibility,
                    2,
                    HIGH_RECEIVE_COUNT,
                    new ThrowingMessageProcessorTest(),
                    CLAZZ));
    assertEquals(
        numMessages,
        metricRepo
            .getCounter(
                AwsMetricConstants.SQS_EVENT_HIGH_NUMBER_OF_RETRIES,
                SqsProcessorFinalizer.class,
                AwsMetricConstants.JOB_TYPE_TAG,
                "DeleteWorkflowJobEvent")
            .count());
  }

  @Getter
  private static class TestableMessageProcessorTest
      implements MaestroEventProcessor<DeleteWorkflowJobEvent> {
    private String inputUsedForProcessing;

    @Override
    public void process(Supplier<DeleteWorkflowJobEvent> messageSupplier) {
      inputUsedForProcessing = messageSupplier.get().getWorkflowId();
    }
  }

  private static class ThrowingMessageProcessorTest
      implements MaestroEventProcessor<DeleteWorkflowJobEvent> {
    static final MaestroRetryableError ex = new MaestroRetryableError("");

    @SuppressWarnings("ReturnValueIgnored")
    @Override
    public void process(Supplier<DeleteWorkflowJobEvent> messageSupplier) {
      messageSupplier.get();
      throw ex;
    }
  }
}
