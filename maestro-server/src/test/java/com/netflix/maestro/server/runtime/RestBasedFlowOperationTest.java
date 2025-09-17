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
package com.netflix.maestro.server.runtime;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.engine.FlowExecutor;
import com.netflix.maestro.flow.properties.FlowEngineProperties;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.web.client.RestTemplate;

public class RestBasedFlowOperationTest extends MaestroBaseTest {

  @Mock private RestTemplate restTemplate;
  @Mock private MaestroFlowDao flowDao;
  @Mock private FlowExecutor flowExecutor;
  @Mock private FlowEngineProperties properties;

  private RestBasedFlowOperation flowOperation;
  private final long groupId = 1L;
  private final String flowReference = "flow1";
  private final String taskReference = "task1";
  private final int actionCode = 123;

  @Before
  public void setUp() {
    when(properties.getEngineAddress()).thenReturn("localhost:8080");
    when(properties.getExpirationDurationInMillis()).thenReturn(300000L);
    flowOperation = new RestBasedFlowOperation(restTemplate, flowDao, flowExecutor, properties);
    when(flowExecutor.wakeUp(groupId, flowReference, taskReference, actionCode)).thenReturn(true);
    when(flowExecutor.wakeUp(anyLong(), anyString(), isNull(), eq(actionCode))).thenReturn(true);
  }

  @Test
  public void testWakeUpWithActionCode() {
    boolean result = flowOperation.wakeUp(groupId, flowReference, taskReference, actionCode);
    assertTrue(result);
    verify(flowExecutor, times(1)).wakeUp(groupId, flowReference, taskReference, actionCode);

    when(flowExecutor.wakeUp(groupId, flowReference, taskReference, actionCode)).thenReturn(false);
    result = flowOperation.wakeUp(groupId, flowReference, taskReference, actionCode);
    assertFalse(result);
    verify(flowExecutor, times(2)).wakeUp(groupId, flowReference, taskReference, actionCode);
  }

  @Test
  public void testWakeUpMultipleFlowsWithActionCode() {
    Set<String> flowReferences = Set.of("flow1", "flow2");
    boolean result = flowOperation.wakeUp(groupId, flowReferences, actionCode);
    assertTrue(result);
    verify(flowExecutor, times(2)).wakeUp(eq(groupId), anyString(), isNull(), eq(actionCode));

    when(flowExecutor.wakeUp(anyLong(), anyString(), isNull(), eq(actionCode))).thenReturn(false);
    result = flowOperation.wakeUp(groupId, flowReferences, actionCode);
    assertFalse(result);
    verify(flowExecutor, times(3)).wakeUp(eq(groupId), anyString(), isNull(), eq(actionCode));
  }
}
