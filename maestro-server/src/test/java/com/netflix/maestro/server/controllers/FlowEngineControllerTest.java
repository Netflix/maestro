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
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.server.controllers.FlowEngineController.StartFlowRequest;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class FlowEngineControllerTest extends MaestroBaseTest {

  @Mock private FlowOperation mockFlowOperation;
  private final long groupId = 123L;
  private final int code = 200;

  private FlowEngineController flowEngineController;

  @Before
  public void before() {
    this.flowEngineController = new FlowEngineController(mockFlowOperation);
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
