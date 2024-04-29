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
package com.netflix.maestro.engine.jobevents;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.events.WorkflowInstanceStatusChangeEvent;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class WorkflowInstanceUpdateJobEventTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowInstanceUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-instance-job-event.json",
            WorkflowInstanceUpdateJobEvent.class);
    assertEquals(
        sampleEvent,
        MAPPER.readValue(
            MAPPER.writeValueAsString(sampleEvent), WorkflowInstanceUpdateJobEvent.class));
  }

  @Test
  public void testToMaestroEventStream() throws Exception {
    WorkflowInstanceUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-instance-job-event.json",
            WorkflowInstanceUpdateJobEvent.class);
    List<WorkflowInstanceStatusChangeEvent> res =
        sampleEvent.toMaestroEventStream("test-cluster").collect(Collectors.toList());
    assertEquals(2, res.size());
    assertEquals("sample-dag-test-3", res.get(0).getWorkflowId());
    assertEquals(1L, res.get(0).getWorkflowInstanceId());
    assertEquals(1, res.get(0).getDepth());
    assertEquals("sample-dag-test-3", res.get(1).getWorkflowId());
    assertEquals(2L, res.get(1).getWorkflowInstanceId());
    assertEquals(0, res.get(1).getDepth());
  }
}
