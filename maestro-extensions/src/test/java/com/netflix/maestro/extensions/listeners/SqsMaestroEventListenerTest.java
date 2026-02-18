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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.extensions.processors.MaestroEventProcessor;
import com.netflix.maestro.models.events.MaestroEvent;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SqsMaestroEventListenerTest extends ExtensionsBaseTest {
  @Mock private MaestroEventProcessor processor;

  private ObjectMapper objectMapper;
  private SqsMaestroEventListener listener;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    objectMapper = MAPPER;
    listener = new SqsMaestroEventListener(processor, objectMapper);
  }

  @Test
  public void testProcessRawPayload() throws Exception {
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
            .syncTime(123456789L)
            .sendTime(123456789L)
            .statusChangeRecords(List.of())
            .build();
    String payload = objectMapper.writeValueAsString(event);

    listener.process(payload);

    ArgumentCaptor<MaestroEvent> captor = ArgumentCaptor.forClass(MaestroEvent.class);
    verify(processor).process(captor.capture());
    MaestroEvent captured = captor.getValue();
    assertThat(captured.getType()).isEqualTo(MaestroEvent.Type.STEP_INSTANCE_STATUS_CHANGE_EVENT);
    assertThat(captured.getWorkflowId()).isEqualTo("test-wf");
  }

  @Test
  public void testProcessSnsWrappedPayload() throws Exception {
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
            .syncTime(123456789L)
            .sendTime(123456789L)
            .statusChangeRecords(List.of())
            .build();
    String innerPayload = objectMapper.writeValueAsString(event);

    // Simulate SNS envelope wrapping the message
    String snsEnvelope =
        objectMapper.writeValueAsString(
            java.util.Map.of(
                "Type", "Notification",
                "MessageId", "msg-123",
                "TopicArn", "arn:aws:sns:us-east-1:000000000000:maestro-test",
                "Message", innerPayload,
                "Timestamp", "2024-01-01T00:00:00.000Z"));

    listener.process(snsEnvelope);

    ArgumentCaptor<MaestroEvent> captor = ArgumentCaptor.forClass(MaestroEvent.class);
    verify(processor).process(captor.capture());
    MaestroEvent captured = captor.getValue();
    assertThat(captured.getType()).isEqualTo(MaestroEvent.Type.STEP_INSTANCE_STATUS_CHANGE_EVENT);
    assertThat(captured.getWorkflowId()).isEqualTo("test-wf");
  }

  @Test
  public void testProcessInvalidPayload() {
    assertThatThrownBy(() -> listener.process("invalid json{"))
        .isInstanceOf(MaestroInternalError.class);
  }

  @Test
  public void testProcessPassesDeserializedEventToProcessor() throws Exception {
    var event =
        StepInstanceStatusChangeEvent.builder()
            .workflowId("workflow-abc")
            .workflowInstanceId(42L)
            .workflowRunId(7L)
            .stepId("my-step")
            .stepAttemptId(3L)
            .workflowUuid("wf-uuid")
            .stepUuid("step-uuid")
            .correlationId("corr-id")
            .stepInstanceId(99L)
            .clusterName("cluster-1")
            .syncTime(100L)
            .sendTime(200L)
            .statusChangeRecords(List.of())
            .build();
    String payload = objectMapper.writeValueAsString(event);

    listener.process(payload);

    verify(processor).process(any(MaestroEvent.class));
  }

  private static org.assertj.core.api.AbstractObjectAssert<?, ?> assertThat(Object actual) {
    return org.assertj.core.api.Assertions.assertThat(actual);
  }
}
