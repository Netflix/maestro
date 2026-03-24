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
package com.netflix.maestro.extensions.listeners;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.extensions.processors.MaestroEventProcessor;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import io.awspring.cloud.sqs.listener.Visibility;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SqsMaestroEventListenerTest extends ExtensionsBaseTest {
  @Mock private MaestroEventProcessor processor;
  @Mock private Acknowledgement acknowledgement;
  @Mock private Visibility visibility;

  private ObjectMapper objectMapper;
  private SqsMaestroEventListener listener;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    objectMapper = MAPPER;
    listener = new SqsMaestroEventListener(processor, objectMapper);
  }

  @Test
  public void testProcessSuccessAcknowledges() throws Exception {
    var event = buildStepEvent("test-wf", "step1");
    String payload = objectMapper.writeValueAsString(event);

    listener.process(payload, acknowledgement, visibility, 1);

    ArgumentCaptor<MaestroEvent> captor = ArgumentCaptor.forClass(MaestroEvent.class);
    verify(processor).process(captor.capture());
    MaestroEvent captured = captor.getValue();
    assertThat(captured.getType()).isEqualTo(MaestroEvent.Type.STEP_INSTANCE_STATUS_CHANGE_EVENT);
    assertThat(captured.getWorkflowId()).isEqualTo("test-wf");
    verify(acknowledgement).acknowledge();
  }

  @Test
  public void testProcessNonRetryableExceptionDeletesMessage() {
    // Non-retryable: deserialization failure should ack (delete) the message
    listener.process("invalid json{", acknowledgement, visibility, 1);

    verify(acknowledgement).acknowledge();
    verify(visibility, never()).changeTo(0);
  }

  @Test
  public void testProcessRetryableExceptionExtendsVisibility() throws Exception {
    var event = buildStepEvent("test-wf", "step1");
    String payload = objectMapper.writeValueAsString(event);
    doThrow(new RuntimeException("transient error")).when(processor).process(any());

    listener.process(payload, acknowledgement, visibility, 1);

    verify(acknowledgement, never()).acknowledge();
    verify(visibility).changeTo(0);
  }

  @Test
  public void testProcessHighReceiveCountRetryableException() throws Exception {
    var event = buildStepEvent("test-wf", "step1");
    String payload = objectMapper.writeValueAsString(event);
    doThrow(new RuntimeException("persistent error")).when(processor).process(any());

    listener.process(payload, acknowledgement, visibility, 100);

    verify(acknowledgement, never()).acknowledge();
    verify(visibility).changeTo(0);
  }

  @Test
  public void testProcessPassesDeserializedEventToProcessor() throws Exception {
    var event = buildStepEvent("workflow-abc", "my-step");
    String payload = objectMapper.writeValueAsString(event);

    listener.process(payload, acknowledgement, visibility, 1);

    verify(processor).process(any(MaestroEvent.class));
    verify(acknowledgement).acknowledge();
  }

  private StepInstanceStatusChangeEvent buildStepEvent(String workflowId, String stepId) {
    return StepInstanceStatusChangeEvent.builder()
        .workflowId(workflowId)
        .workflowInstanceId(1L)
        .workflowRunId(1L)
        .stepId(stepId)
        .stepAttemptId(1L)
        .workflowUuid("uuid1")
        .stepUuid("suuid1")
        .correlationId("corr1")
        .stepInstanceId(1L)
        .clusterName("test-cluster")
        .syncTime(123456789L)
        .sendTime(123456789L)
        .statusChangeRecords(List.of())
        .build();
  }

  private static org.assertj.core.api.AbstractObjectAssert<?, ?> assertThat(Object actual) {
    return org.assertj.core.api.Assertions.assertThat(actual);
  }
}
