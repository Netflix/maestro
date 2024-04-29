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
package com.netflix.maestro.engine.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.Collections;
import org.junit.BeforeClass;
import org.junit.Test;

public class WorkflowRuntimeSummaryTest extends MaestroBaseTest {

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  public void testRoundTripSerdeBase(String fileName) throws Exception {
    WorkflowRuntimeSummary expected = loadObject(fileName, WorkflowRuntimeSummary.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    WorkflowRuntimeSummary actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), WorkflowRuntimeSummary.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    testRoundTripSerdeBase("fixtures/instances/sample-workflow-runtime-summary.json");
    testRoundTripSerdeBase(
        "fixtures/instances/sample-workflow-runtime-summary-with-rollup-base.json");
  }

  @Test
  public void testUpdateRuntimeState() {
    WorkflowRuntimeSummary summary = new WorkflowRuntimeSummary();
    summary.updateRuntimeState(WorkflowInstance.Status.IN_PROGRESS, null, 123456L);
    assertEquals(123456L, summary.getStartTime().longValue());
    assertNull(summary.getEndTime());
    assertEquals(WorkflowInstance.Status.IN_PROGRESS, summary.getInstanceStatus());
    assertNull(summary.getRuntimeOverview());

    WorkflowRuntimeOverview overview =
        WorkflowRuntimeOverview.of(
            1,
            singletonEnumMap(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(1L)),
            null);
    summary.updateRuntimeState(WorkflowInstance.Status.SUCCEEDED, overview, 654321L);
    assertEquals(123456L, summary.getStartTime().longValue());
    assertEquals(654321L, summary.getEndTime().longValue());
    assertEquals(WorkflowInstance.Status.SUCCEEDED, summary.getInstanceStatus());
    assertEquals(overview, summary.getRuntimeOverview());
  }

  @Test
  public void testAddTimeline() {
    WorkflowRuntimeSummary summary = new WorkflowRuntimeSummary();
    TimelineEvent event = TimelineLogEvent.info("hello world");
    summary.addTimeline(event);
    assertEquals(Collections.singletonList(event), summary.getTimeline().getTimelineEvents());
  }

  @Test
  public void testAddDuplicateTimelineEvents() {
    WorkflowRuntimeSummary summary = new WorkflowRuntimeSummary();
    TimelineEvent event = TimelineLogEvent.info("hello world");
    summary.addTimeline(event);
    summary.addTimeline(TimelineLogEvent.info("hello world"));
    summary.addTimeline(TimelineLogEvent.info("hello world"));
    summary.addTimeline(TimelineLogEvent.info("hello world"));
    assertEquals(Collections.singletonList(event), summary.getTimeline().getTimelineEvents());
  }
}
