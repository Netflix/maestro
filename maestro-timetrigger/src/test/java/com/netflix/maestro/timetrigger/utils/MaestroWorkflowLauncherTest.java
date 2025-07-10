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
package com.netflix.maestro.timetrigger.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.trigger.CronTimeTrigger;
import com.netflix.maestro.timetrigger.models.PlannedTimeTriggerExecution;
import com.netflix.maestro.timetrigger.models.TimeTriggerWithWatermark;
import java.util.Date;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class MaestroWorkflowLauncherTest {
  private WorkflowActionHandler actionHandler;
  private MaestroWorkflowLauncher workflowLauncher;

  @Before
  public void setUp() {
    actionHandler = Mockito.mock(WorkflowActionHandler.class);
    workflowLauncher = new MaestroWorkflowLauncher(actionHandler);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEmptyStartWorkflowBatchRuns() {
    String workflowId = "workflowId";
    String version = "version";
    String workflowTriggerUuid = "workflowTriggerUuid";
    workflowLauncher.startWorkflowBatchRuns(workflowId, version, workflowTriggerUuid, List.of());

    var requestCaptor = ArgumentCaptor.forClass(List.class);
    verify(actionHandler, times(1))
        .startBatch(eq(workflowId), eq(version), requestCaptor.capture());
    var requests = (List<RunRequest>) requestCaptor.getValue();
    assertTrue(requests.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStartWorkflowBatchRuns() {
    String workflowId = "workflowId";
    String version = "version";
    String workflowTriggerUuid = "workflowTriggerUuid";
    workflowLauncher.startWorkflowBatchRuns(
        workflowId,
        version,
        workflowTriggerUuid,
        List.of(
            new PlannedTimeTriggerExecution(
                TimeTriggerWithWatermark.builder()
                    .timeTrigger(new CronTimeTrigger())
                    .lastTriggerTimestamp(123456)
                    .build(),
                new Date(123456789))));

    var requestCaptor = ArgumentCaptor.forClass(List.class);
    verify(actionHandler, times(1))
        .startBatch(eq(workflowId), eq(version), requestCaptor.capture());
    assertEquals(1, requestCaptor.getValue().size());
    var request = (RunRequest) requestCaptor.getValue().getFirst();
    assertEquals(123456789L, ((TimeInitiator) request.getInitiator()).getTriggerTime().longValue());
    assertTrue(request.isPersistFailedRun());
    assertEquals(1, request.getRunParams().size());
    assertEquals(
        123456789L, request.getRunParams().get("RUN_TS").asLongParamDef().getValue().longValue());
  }
}
