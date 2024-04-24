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

import com.netflix.maestro.MaestroBaseTest;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowRollupOverviewTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    WorkflowRollupOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-rollup-overview.json",
            WorkflowRollupOverview.class);
    assertEquals(
        overview,
        MAPPER.readValue(MAPPER.writeValueAsString(overview), WorkflowRollupOverview.class));
  }

  @Test
  public void testAggregate() throws Exception {
    WorkflowRollupOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-rollup-overview.json",
            WorkflowRollupOverview.class);

    WorkflowRollupOverview.CountReference ref1 = new WorkflowRollupOverview.CountReference();
    ref1.setCnt(1);
    WorkflowRollupOverview.CountReference ref2 = new WorkflowRollupOverview.CountReference();
    ref2.setCnt(1);
    ref2.setRef(
        Collections.singletonMap("maestro_foreach_123", Collections.singletonList("123:job1:1")));
    Map<StepInstance.Status, WorkflowRollupOverview.CountReference> newOverview = new HashMap<>();
    newOverview.put(StepInstance.Status.CREATED, ref1);
    newOverview.put(StepInstance.Status.RUNNING, ref2);

    WorkflowRollupOverview actual = overview.aggregate(WorkflowRollupOverview.of(2, newOverview));

    assertEquals(10081, actual.getTotalLeafCount());
    assertEquals(ref1, actual.getOverview().get(StepInstance.Status.CREATED));
    assertEquals(28, actual.getOverview().get(StepInstance.Status.RUNNING).getCnt());
    assertEquals(
        Collections.singletonList("123:job1:1"),
        actual.getOverview().get(StepInstance.Status.RUNNING).getRef().get("maestro_foreach_123"));
  }

  @Test
  public void testSegregate() throws Exception {
    WorkflowRollupOverview overview =
        loadObject(
            "fixtures/instances/sample-workflow-rollup-overview.json",
            WorkflowRollupOverview.class);

    WorkflowRollupOverview.CountReference ref1 = new WorkflowRollupOverview.CountReference();
    ref1.setCnt(1);
    ref1.setRef(
        Collections.singletonMap(
            "maestro_foreach_2b6e7ab1ae5975490c94a12ae63555c9:1",
            Collections.singletonList("5000:job.2:1")));
    WorkflowRollupOverview.CountReference ref2 = new WorkflowRollupOverview.CountReference();
    ref2.setCnt(1);
    Map<StepInstance.Status, WorkflowRollupOverview.CountReference> newOverview = new HashMap<>();
    newOverview.put(StepInstance.Status.RUNNING, ref1);
    newOverview.put(StepInstance.Status.INITIALIZED, ref2);

    overview.segregate(WorkflowRollupOverview.of(2, newOverview));

    assertEquals(10077, overview.getTotalLeafCount());
    assertEquals(66, overview.getOverview().get(StepInstance.Status.INITIALIZED).getCnt());
    assertEquals(26, overview.getOverview().get(StepInstance.Status.RUNNING).getCnt());
    assertFalse(
        overview
            .getOverview()
            .get(StepInstance.Status.RUNNING)
            .getRef()
            .get("maestro_foreach_2b6e7ab1ae5975490c94a12ae63555c9:1")
            .remove("5000:job.2:1"));
  }
}
