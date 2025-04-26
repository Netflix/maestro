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
package com.netflix.maestro.queue.jobevents;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.events.MaestroEvent;
import org.junit.Test;

public class WorkflowVersionUpdateJobEventTest extends MaestroBaseTest {

  @Test
  public void testDefinitionChangeEventRoundTripSerde() throws Exception {
    WorkflowVersionUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-definition-job-event.json",
            WorkflowVersionUpdateJobEvent.class);
    assertEquals(
        sampleEvent,
        MaestroBaseTest.MAPPER.readValue(
            MaestroBaseTest.MAPPER.writeValueAsString(sampleEvent),
            WorkflowVersionUpdateJobEvent.class));

    MaestroEvent event = sampleEvent.toMaestroEvent("test-cluster");
    assertEquals(MaestroEvent.Type.WORKFLOW_DEFINITION_CHANGE_EVENT, event.getType());
  }

  @Test
  public void testVersionChangeEventRoundTripSerde() throws Exception {
    WorkflowVersionUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-version-job-event.json",
            WorkflowVersionUpdateJobEvent.class);
    assertEquals(
        sampleEvent,
        MaestroBaseTest.MAPPER.readValue(
            MaestroBaseTest.MAPPER.writeValueAsString(sampleEvent),
            WorkflowVersionUpdateJobEvent.class));

    MaestroEvent event = sampleEvent.toMaestroEvent("test-cluster");
    assertEquals(MaestroEvent.Type.WORKFLOW_VERSION_CHANGE_EVENT, event.getType());
  }

  @Test
  public void testPropertiesChangeEventRoundTripSerde() throws Exception {
    WorkflowVersionUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-properties-job-event.json",
            WorkflowVersionUpdateJobEvent.class);
    assertEquals(
        sampleEvent,
        MaestroBaseTest.MAPPER.readValue(
            MaestroBaseTest.MAPPER.writeValueAsString(sampleEvent),
            WorkflowVersionUpdateJobEvent.class));

    MaestroEvent event = sampleEvent.toMaestroEvent("test-cluster");
    assertEquals(MaestroEvent.Type.WORKFLOW_PROPERTIES_CHANGE_EVENT, event.getType());
  }

  @Test
  public void testActivationChangeEventRoundTripSerde() throws Exception {
    WorkflowVersionUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-activation-job-event.json",
            WorkflowVersionUpdateJobEvent.class);
    assertEquals(
        sampleEvent,
        MaestroBaseTest.MAPPER.readValue(
            MaestroBaseTest.MAPPER.writeValueAsString(sampleEvent),
            WorkflowVersionUpdateJobEvent.class));

    MaestroEvent event = sampleEvent.toMaestroEvent("test-cluster");
    assertEquals(MaestroEvent.Type.WORKFLOW_ACTIVATION_CHANGE_EVENT, event.getType());
  }

  @Test
  public void testDeactivationChangeEventRoundTripSerde() throws Exception {
    WorkflowVersionUpdateJobEvent sampleEvent =
        loadObject(
            "fixtures/jobevents/sample-workflow-deactivation-job-event.json",
            WorkflowVersionUpdateJobEvent.class);
    assertEquals(
        sampleEvent,
        MaestroBaseTest.MAPPER.readValue(
            MaestroBaseTest.MAPPER.writeValueAsString(sampleEvent),
            WorkflowVersionUpdateJobEvent.class));

    MaestroEvent event = sampleEvent.toMaestroEvent("test-cluster");
    assertEquals(MaestroEvent.Type.WORKFLOW_DEACTIVATION_CHANGE_EVENT, event.getType());
  }
}
