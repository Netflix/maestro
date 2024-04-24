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
package com.netflix.maestro.models.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import java.util.Collections;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowRuntimeOverviewTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowRuntimeOverview request =
        loadObject(
            "fixtures/instances/sample-workflow-runtime-overview.json",
            WorkflowRuntimeOverview.class);
    assertEquals(
        request,
        MAPPER.readValue(MAPPER.writeValueAsString(request), WorkflowRuntimeOverview.class));
  }

  @Test
  public void testExistsNotCreatedStep() throws Exception {
    WorkflowRuntimeOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-runtime-overview.json",
            WorkflowRuntimeOverview.class);
    assertFalse(overview.existsNotCreatedStep());

    overview.setTotalStepCount(2);
    assertTrue(overview.existsNotCreatedStep());

    overview
        .getStepOverview()
        .put(StepInstance.Status.NOT_CREATED, WorkflowStepStatusSummary.of(1));
    assertTrue(overview.existsNotCreatedStep());

    overview.getStepOverview().put(StepInstance.Status.CREATED, WorkflowStepStatusSummary.of(0));
    assertTrue(overview.existsNotCreatedStep());
  }

  @Test
  public void testExistsCreatedStep() throws Exception {
    WorkflowRuntimeOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-runtime-overview.json",
            WorkflowRuntimeOverview.class);
    assertTrue(overview.existsCreatedStep());

    overview.setTotalStepCount(2);
    assertTrue(overview.existsCreatedStep());

    overview.setStepOverview(
        singletonEnumMap(StepInstance.Status.NOT_CREATED, WorkflowStepStatusSummary.of(1)));
    assertFalse(overview.existsCreatedStep());

    overview.getStepOverview().put(StepInstance.Status.CREATED, WorkflowStepStatusSummary.of(0));
    assertFalse(overview.existsCreatedStep());
  }

  @Test
  public void testDecodeStepOverview() throws Exception {
    WorkflowRuntimeOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-runtime-overview.json",
            WorkflowRuntimeOverview.class);
    assertFalse(overview.existsNotCreatedStep());

    WorkflowInstance instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);

    Map<String, StepRuntimeState> actual = overview.decodeStepOverview(instance.getRuntimeDag());
    assertEquals(1, overview.decodeStepOverview(instance.getRuntimeDag()).size());
    assertEquals(StepInstance.Status.RUNNING, actual.get("job3").getStatus());
    assertEquals(1647977244273L, actual.get("job3").getStartTime().longValue());
  }

  @Test
  public void testDecodeInvalidStepOverview() throws Exception {
    WorkflowRuntimeOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-runtime-overview.json",
            WorkflowRuntimeOverview.class);
    assertFalse(overview.existsNotCreatedStep());
    AssertHelper.assertThrows(
        "invalid dag or overview",
        NullPointerException.class,
        "cannot find step id for stepInfo",
        () -> overview.decodeStepOverview(Collections.emptyMap()));
  }
}
