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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.extensions.ExtensionsBaseTest;
import com.netflix.maestro.extensions.models.StepEventHandlerInput;
import com.netflix.maestro.extensions.provider.MaestroDataProvider;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.events.StepInstanceStatusChangeEvent;
import com.netflix.maestro.models.instance.WorkflowInstance;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StepEventPreprocessorTest extends ExtensionsBaseTest {
  @Mock private MaestroDataProvider maestroDataProvider;
  @Mock private MaestroMetrics metrics;
  @Mock private WorkflowInstance workflowInstance;

  private StepEventPreprocessor preprocessor;

  @Before
  public void setup() {
    MockitoAnnotations.openMocks(this);
    preprocessor = new StepEventPreprocessor(maestroDataProvider, metrics);
  }

  @Test
  public void testPreprocessReturnsStepEventHandlerInput() {
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
    when(maestroDataProvider.getWorkflowInstance("test-wf", 1L, 1L)).thenReturn(workflowInstance);

    StepEventHandlerInput result = preprocessor.preprocess(event);

    assertThat(result).isNotNull();
    assertThat(result.workflowInstance()).isEqualTo(workflowInstance);
    assertThat(result.stepId()).isEqualTo("step1");
    assertThat(result.stepAttemptId()).isEqualTo(1L);
  }

  @Test
  public void testPreprocessReturnsNullWhenNotFound() {
    var event =
        StepInstanceStatusChangeEvent.builder()
            .workflowId("missing-wf")
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
    when(maestroDataProvider.getWorkflowInstance("missing-wf", 1L, 1L))
        .thenThrow(new MaestroNotFoundException("not found"));

    StepEventHandlerInput result = preprocessor.preprocess(event);

    assertThat(result).isNull();
  }
}
