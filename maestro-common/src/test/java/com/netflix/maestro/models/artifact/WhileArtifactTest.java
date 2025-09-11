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
package com.netflix.maestro.models.artifact;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import org.junit.Test;

public class WhileArtifactTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerde() throws Exception {
    WhileArtifact request =
        loadObject("fixtures/artifact/sample-while-artifact.json", WhileArtifact.class);
    assertEquals(
        request, MAPPER.readValue(MAPPER.writeValueAsString(request), WhileArtifact.class));
  }

  @Test
  public void testGetOverallRollup() {
    WhileArtifact artifact = new WhileArtifact();

    WorkflowRollupOverview rollup = new WorkflowRollupOverview();
    rollup.setTotalLeafCount(10);
    artifact.setRollup(rollup);

    WorkflowRuntimeOverview lastOverview = new WorkflowRuntimeOverview();
    WorkflowRollupOverview lastRollup = new WorkflowRollupOverview();
    lastRollup.setTotalLeafCount(5);
    lastOverview.setRollupOverview(lastRollup);
    artifact.setLastOverview(lastOverview);

    WorkflowRollupOverview overall = artifact.getOverallRollup();

    assertNotNull(overall);
    assertEquals(15, overall.getTotalLeafCount());
  }
}
