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
package com.netflix.maestro.server.controllers;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.server.controllers.FlowEngineController.StartFlowRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class FlowEngineControllerTest extends MaestroBaseTest {

  @Mock private FlowOperation mockFlowOperation;
  private final long groupId = 123L;
  private final int code = 200;

  private FlowEngineController flowEngineController;

  @Before
  public void before() {
    this.flowEngineController = new FlowEngineController(mockFlowOperation, MAPPER);
  }

  @Test
  public void testStartFlow() {
    String flowReference = "test-flow-ref";
    String flowId = "test-flow-id";
    StartFlowRequest request = new StartFlowRequest(flowId, null, Map.of());
    String expectedResult = "flow-execution-id";

    when(mockFlowOperation.startFlow(
            eq(groupId), eq(flowId), eq(flowReference), isNull(), eq(Map.of())))
        .thenReturn(expectedResult);

    String result = flowEngineController.startFlow(groupId, flowReference, request);

    verify(mockFlowOperation, times(1))
        .startFlow(eq(groupId), eq(flowId), eq(flowReference), isNull(), eq(Map.of()));
    Assert.assertEquals(expectedResult, result);
  }

  @Test
  public void testStartFlowReTypesWorkflowSummaryFromMap() throws Exception {
    String flowReference = "test-flow-ref";
    String flowId = "test-flow-id";

    // Simulate the cross-node path: the request body was deserialized into a Map<String, Object>,
    // so the workflow summary value arrives as an untyped Map, not a WorkflowSummary.
    WorkflowSummary summary =
        loadObject("fixtures/parameters/sample-wf-summary-params.json", WorkflowSummary.class);
    Object summaryAsMap = MAPPER.convertValue(summary, Map.class);
    Assert.assertFalse(summaryAsMap instanceof WorkflowSummary);
    Map<String, Object> flowInput = new HashMap<>();
    flowInput.put(Constants.WORKFLOW_SUMMARY_FIELD, summaryAsMap);

    flowEngineController.startFlow(
        groupId, flowReference, new StartFlowRequest(flowId, null, flowInput));

    // The controller must hand a typed WorkflowSummary to the flow, not the raw map.
    ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
    verify(mockFlowOperation, times(1))
        .startFlow(eq(groupId), eq(flowId), eq(flowReference), isNull(), captor.capture());
    Object handed = captor.getValue().get(Constants.WORKFLOW_SUMMARY_FIELD);
    Assert.assertTrue(handed instanceof WorkflowSummary);
    Assert.assertEquals(MAPPER.writeValueAsString(summary), MAPPER.writeValueAsString(handed));
  }

  @Test
  public void testStartFlowLeavesTypedWorkflowSummaryUntouched() {
    String flowReference = "test-flow-ref";
    String flowId = "test-flow-id";

    WorkflowSummary summary = new WorkflowSummary();
    Map<String, Object> flowInput = new HashMap<>();
    flowInput.put(Constants.WORKFLOW_SUMMARY_FIELD, summary);

    flowEngineController.startFlow(
        groupId, flowReference, new StartFlowRequest(flowId, null, flowInput));

    ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
    verify(mockFlowOperation, times(1))
        .startFlow(eq(groupId), eq(flowId), eq(flowReference), isNull(), captor.capture());
    // already typed, so the same instance passes through without a conversion.
    Assert.assertSame(summary, captor.getValue().get(Constants.WORKFLOW_SUMMARY_FIELD));
  }

  @Test
  public void testWakeUpSingleTask() {
    String flowReference = "test-flow-ref";
    String taskReference = "test-task-ref";
    when(mockFlowOperation.wakeUp(eq(groupId), eq(flowReference), eq(taskReference), eq(code)))
        .thenReturn(true);

    Boolean result = flowEngineController.wakeUp(groupId, flowReference, taskReference, code);

    verify(mockFlowOperation, times(1))
        .wakeUp(eq(groupId), eq(flowReference), eq(taskReference), eq(code));
    assert result.equals(true);
  }

  @Test
  public void testWakeUpMultipleFlows() {
    Set<String> refs = Set.of("flow-ref-1", "flow-ref-2", "flow-ref-3");
    when(mockFlowOperation.wakeUp(eq(groupId), eq(refs), eq(code))).thenReturn(true);

    Boolean result = flowEngineController.wakeUp(groupId, code, refs);

    verify(mockFlowOperation, times(1)).wakeUp(eq(groupId), eq(refs), eq(code));
    assert result.equals(true);
  }
}
