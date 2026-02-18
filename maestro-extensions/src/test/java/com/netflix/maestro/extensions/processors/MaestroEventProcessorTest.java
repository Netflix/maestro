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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.extensions.handlers.ForeachFlatteningHandler;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.events.WorkflowInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.WorkflowInstance;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MaestroEventProcessorTest extends ExtensionsBaseTest {
  @Mock private StepEventPreprocessor stepEventPreprocessor;
  @Mock private ForeachFlatteningHandler foreachFlatteningHandler;
  @Mock private MaestroMetrics metrics;
  @Mock private WorkflowInstance workflowInstance;

  private MaestroEventProcessor processor;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    processor = new MaestroEventProcessor(stepEventPreprocessor, foreachFlatteningHandler, metrics);
  }

  @Test
  public void testProcessStepInstanceStatusChangeEvent() {
    var event =
        StepInstanceStatusChangeEvent.builder()
            .workflowId("test-wf")
            .workflowInstanceId(1L)
            .workflowRunId(1L)
            .stepId("step1")
            .stepAttemptId(1L)
            .workflowUuid("uuid1")
            .stepUuid("suuid1")
            .correlationId("corr1")
            .stepInstanceId(1L)
            .clusterName("test-cluster")
            .syncTime(System.currentTimeMillis())
            .sendTime(System.currentTimeMillis())
            .statusChangeRecords(java.util.List.of())
            .build();
    var input = new StepEventHandlerInput(workflowInstance, "step1", 1L);
    when(stepEventPreprocessor.preprocess(event)).thenReturn(input);

    processor.process(event);

    verify(stepEventPreprocessor).preprocess(event);
    verify(foreachFlatteningHandler).process(input);
  }

  @Test
  public void testProcessStepEventWithNullPreprocessResult() {
    var event =
        StepInstanceStatusChangeEvent.builder()
            .workflowId("test-wf")
            .workflowInstanceId(1L)
            .workflowRunId(1L)
            .stepId("step1")
            .stepAttemptId(1L)
            .workflowUuid("uuid1")
            .stepUuid("suuid1")
            .correlationId("corr1")
            .stepInstanceId(1L)
            .clusterName("test-cluster")
            .syncTime(System.currentTimeMillis())
            .sendTime(System.currentTimeMillis())
            .statusChangeRecords(java.util.List.of())
            .build();
    when(stepEventPreprocessor.preprocess(event)).thenReturn(null);

    processor.process(event);

    verify(stepEventPreprocessor).preprocess(event);
    verify(foreachFlatteningHandler, never()).process(any(StepEventHandlerInput.class));
  }

  @Test
  public void testProcessNonStepEventIgnored() {
    var event =
        WorkflowInstanceStatusChangeEvent.builder()
            .workflowId("test-wf")
            .workflowName("test-wf")
            .workflowInstanceId(1L)
            .workflowRunId(1L)
            .workflowUuid("uuid1")
            .correlationId("corr1")
            .clusterName("test-cluster")
            .syncTime(System.currentTimeMillis())
            .sendTime(System.currentTimeMillis())
            .eventTime(System.currentTimeMillis())
            .oldStatus(com.netflix.maestro.models.instance.WorkflowInstance.Status.CREATED)
            .newStatus(com.netflix.maestro.models.instance.WorkflowInstance.Status.IN_PROGRESS)
            .build();

    processor.process(event);

    verify(stepEventPreprocessor, never()).preprocess(any());
    verify(foreachFlatteningHandler, never()).process(any(StepEventHandlerInput.class));
  }

  @Test
  public void testProcessHandlesExceptionGracefully() {
    var event =
        StepInstanceStatusChangeEvent.builder()
            .workflowId("test-wf")
            .workflowInstanceId(1L)
            .workflowRunId(1L)
            .stepId("step1")
            .stepAttemptId(1L)
            .workflowUuid("uuid1")
            .stepUuid("suuid1")
            .correlationId("corr1")
            .stepInstanceId(1L)
            .clusterName("test-cluster")
            .syncTime(System.currentTimeMillis())
            .sendTime(System.currentTimeMillis())
            .statusChangeRecords(java.util.List.of())
            .build();
    when(stepEventPreprocessor.preprocess(event)).thenThrow(new RuntimeException("test error"));

    // Should not throw - errors are caught and logged
    processor.process(event);
  }
}
