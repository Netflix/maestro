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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;

public class ForeachStepOverviewTest extends MaestroBaseTest {
  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    assertEquals(
        overview, MAPPER.readValue(MAPPER.writeValueAsString(overview), ForeachStepOverview.class));
  }

  @Test
  public void testAddTerminalOne() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    WorkflowRollupOverview rollupOverview =
        WorkflowRollupOverview.of(1, Collections.singletonMap(StepInstance.Status.SUCCEEDED, ref));
    overview.addOne(80333, WorkflowInstance.Status.SUCCEEDED, rollupOverview);
    assertEquals(singletonEnumMap(WorkflowInstance.Status.SUCCEEDED, 80154L), overview.getStats());

    assertEquals(160307, overview.getRollup().getTotalLeafCount());
    ref.setCnt(160307);
    assertEquals(
        singletonEnumMap(StepInstance.Status.SUCCEEDED, ref), overview.getRollup().getOverview());

    assertTrue(overview.statusExistInIterations(WorkflowInstance.Status.SUCCEEDED));
    assertFalse(overview.statusExistInIterations(WorkflowInstance.Status.FAILED));
    assertEquals(10116, overview.getRunningStatsCount(true));
    assertEquals(332, overview.getRunningStatsCount(false));
  }

  @Test
  public void testAddRunningOne() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    WorkflowRollupOverview rollupOverview =
        WorkflowRollupOverview.of(1, Collections.singletonMap(StepInstance.Status.RUNNING, ref));
    overview.addOne(80333, WorkflowInstance.Status.IN_PROGRESS, rollupOverview);
    assertEquals(
        207L, overview.getRunningStats().get(WorkflowInstance.Status.IN_PROGRESS).longValue());

    assertEquals(517L, overview.getRunningRollup().getTotalLeafCount());
    ref.setCnt(60);
    assertEquals(ref, overview.getRunningRollup().getOverview().get(StepInstance.Status.RUNNING));

    assertTrue(overview.statusExistInIterations(WorkflowInstance.Status.SUCCEEDED));
    assertFalse(overview.statusExistInIterations(WorkflowInstance.Status.FAILED));
    assertEquals(10116, overview.getRunningStatsCount(true));
    assertEquals(333, overview.getRunningStatsCount(false));
  }

  @Test
  public void testRefresh() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    WorkflowRollupOverview rollupOverview =
        WorkflowRollupOverview.of(1, Collections.singletonMap(StepInstance.Status.SUCCEEDED, ref));
    overview.addOne(80111, WorkflowInstance.Status.SUCCEEDED, rollupOverview);
    rollupOverview =
        WorkflowRollupOverview.of(1, Collections.singletonMap(StepInstance.Status.SUCCEEDED, ref));
    overview.addOne(80333, WorkflowInstance.Status.SUCCEEDED, rollupOverview);

    overview.refreshDetail();
    ForeachDetails detail = overview.getDetails();
    assertEquals(1, detail.getInfo().size());
    assertEquals(
        Arrays.asList(
            new ForeachDetails.Interval(80110L, 80115L),
            new ForeachDetails.Interval(80333L, 80333L)),
        detail.getInfo().get(WorkflowInstance.Status.SUCCEEDED));
    assertEquals(
        Arrays.asList(80110L, 80111L, 80112L, 80113L, 80114L, 80115L, 80333L),
        detail.flatten(e -> true).get(WorkflowInstance.Status.SUCCEEDED));
    assertEquals(
        Arrays.asList(80110L, 80115L),
        detail.getInfo().get(WorkflowInstance.Status.SUCCEEDED).get(0).toJson());
    assertEquals(80333L, detail.getInfo().get(WorkflowInstance.Status.SUCCEEDED).get(1).toJson());
  }

  @Test
  public void testisForeachIterationRestartable() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    assertFalse(overview.isForeachIterationRestartable(123L));
    overview.addOne(123L, WorkflowInstance.Status.FAILED, null);
    overview.refreshDetail();
    assertTrue(overview.isForeachIterationRestartable(123L));
    assertEquals(0, overview.getRunningStatsCount(true));
    overview.updateForRestart(
        123L, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED, null);
    overview.refreshDetail();
    assertFalse(overview.isForeachIterationRestartable(123L));
    assertEquals(79993, overview.getRunningStatsCount(true));
  }

  @Test
  public void testUpdateForRestart() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    AssertHelper.assertThrows(
        "should throw exception for invalid restart",
        IllegalArgumentException.class,
        "Invalid: pending action tries to restart a non-restartable iteration",
        () ->
            overview.updateForRestart(
                123L, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED, null));

    overview.addOne(123L, WorkflowInstance.Status.FAILED, null);
    overview.refreshDetail();
    assertEquals(1L, overview.getStats().get(WorkflowInstance.Status.FAILED).longValue());
    assertFalse(overview.getStats().containsKey(WorkflowInstance.Status.CREATED));
    assertEquals(
        Collections.singletonList(new ForeachDetails.Interval(123L, 123L)),
        overview.getDetails().getInfo().get(WorkflowInstance.Status.FAILED));
    assertFalse(overview.getDetails().getInfo().containsKey(WorkflowInstance.Status.CREATED));

    WorkflowRollupOverview rollup = new WorkflowRollupOverview();
    rollup.setTotalLeafCount(1);
    overview.updateForRestart(
        123L, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED, rollup);

    assertFalse(overview.isForeachIterationRestartable(123L));
    assertFalse(overview.getStats().containsKey(WorkflowInstance.Status.FAILED));
    assertEquals(127L, overview.getRunningStats().get(WorkflowInstance.Status.CREATED).longValue());
    assertEquals(
        Collections.singletonList(new ForeachDetails.Interval(123L, 123L)),
        overview.getDetails().getPendingInfo().get(WorkflowInstance.Status.CREATED));
    assertFalse(overview.getDetails().getInfo().containsKey(WorkflowInstance.Status.FAILED));
    assertEquals(Collections.singleton(123L), overview.getRestartInfo());
    assertEquals(160305, overview.getRollup().getTotalLeafCount());
  }

  @Test
  public void testGetFirstRestartIterationId() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    assertEquals(0, overview.getFirstRestartIterationId());

    overview.addOne(23L, WorkflowInstance.Status.FAILED, null);
    overview.addOne(123L, WorkflowInstance.Status.FAILED, null);
    overview.refreshDetail();
    overview.updateForRestart(
        23L, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED, null);
    overview.updateForRestart(
        123L, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED, null);

    assertEquals(23L, overview.getFirstRestartIterationId());
  }

  @Test
  public void testGetSkippedIterationsWithCheckpoint() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    assertTrue(overview.getSkippedIterationsWithCheckpoint().isEmpty());

    overview.setCheckpoint(80115);
    assertEquals(Collections.singleton(80115L), overview.getSkippedIterationsWithCheckpoint());

    overview.addOne(123L, WorkflowInstance.Status.FAILED, null);
    overview.refreshDetail();
    overview.updateForRestart(
        123L, WorkflowInstance.Status.CREATED, WorkflowInstance.Status.FAILED, null);

    assertEquals(
        new HashSet<>(Arrays.asList(123L, 80115L)), overview.getSkippedIterationsWithCheckpoint());
  }

  @Test
  public void testInitiateAndGetByPrevMaxIterationId() throws Exception {
    ForeachStepOverview prev =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview-with-failed.json",
            ForeachStepOverview.class);
    ForeachStepOverview overview = new ForeachStepOverview();
    assertEquals(80121, overview.initiateAndGetByPrevMaxIterationId(prev, 0));
    assertEquals(
        Collections.singletonMap(
            WorkflowInstance.Status.SUCCEEDED,
            Arrays.asList(80110L, 80112L, 80113L, 80114L, 80115L)),
        overview.getDetails().flatten(e -> true));
    assertEquals(0, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    assertEquals(80121, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    Map<WorkflowInstance.Status, List<Long>> detailsMap = new HashMap<>();
    detailsMap.put(
        WorkflowInstance.Status.SUCCEEDED, Arrays.asList(80110L, 80112L, 80113L, 80114L, 80115L));
    detailsMap.put(WorkflowInstance.Status.FAILED, Arrays.asList(80117L, 80119L, 80120L, 80121L));
    assertEquals(detailsMap, overview.getDetails().flatten(e -> true));
    assertEquals(0, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    assertEquals(0, overview.initiateAndGetByPrevMaxIterationId(null, 80115));
    assertEquals(0, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    prev.getDetails().add(1, WorkflowInstance.Status.SUCCEEDED);
    prev.getDetails().add(2, WorkflowInstance.Status.STOPPED);
    prev.refreshDetail();
    assertEquals(80121, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    assertEquals(2, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    prev.getDetails().getInfo().clear();
    prev.getDetails()
        .getInfo()
        .put(
            WorkflowInstance.Status.STOPPED,
            Collections.singletonList(new ForeachDetails.Interval(1, 10000)));
    assertEquals(10000, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    assertEquals(10000, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    prev.getDetails().getInfo().clear();
    assertEquals(0, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    assertEquals(0, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    prev.getDetails()
        .getInfo()
        .put(
            WorkflowInstance.Status.SUCCEEDED,
            Collections.singletonList(new ForeachDetails.Interval(1, 10000)));
    assertEquals(10000, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    assertEquals(10000, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    prev.getDetails().getInfo().clear();
    prev.getDetails().add(1, WorkflowInstance.Status.SUCCEEDED);
    prev.getDetails().add(2, WorkflowInstance.Status.STOPPED);
    prev.getDetails().add(3, WorkflowInstance.Status.SUCCEEDED);
    prev.refreshDetail();
    assertEquals(3, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    assertEquals(3, overview.getCheckpoint());

    overview = new ForeachStepOverview();
    prev.getDetails().getInfo().clear();
    prev.getDetails().add(1, WorkflowInstance.Status.SUCCEEDED);
    prev.getDetails().add(2, WorkflowInstance.Status.STOPPED);
    prev.refreshDetail();
    assertEquals(2, overview.initiateAndGetByPrevMaxIterationId(prev, 80115));
    assertEquals(2, overview.getCheckpoint());
  }

  @Test
  public void testGetSkippedIterationsInRange() throws Exception {
    ForeachStepOverview overview =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);
    assertEquals(Collections.emptySet(), overview.getSkippedIterationsInRange(10, null));
    assertEquals(Collections.emptySet(), overview.getSkippedIterationsInRange(100, 10L));
    assertEquals(
        Collections.singleton(80115L), overview.getSkippedIterationsInRange(80115, 80115L));

    overview = new ForeachStepOverview();
    assertEquals(Collections.emptySet(), overview.getSkippedIterationsInRange(10, 10L));
  }

  @Test
  public void testInitiateStepRollup() throws Exception {
    ForeachStepOverview prev =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    WorkflowRollupOverview aggregatedRollupsPrevRun = new WorkflowRollupOverview();
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(10);
    aggregatedRollupsPrevRun.setTotalLeafCount(10);
    aggregatedRollupsPrevRun.setOverview(singletonEnumMap(StepInstance.Status.SUCCEEDED, ref));

    ForeachStepOverview newRun = new ForeachStepOverview();
    newRun.initiateStepRollup(prev.getOverallRollup(), aggregatedRollupsPrevRun);
    assertEquals(160812, newRun.getOverallRollup().getTotalLeafCount());
    assertEquals(
        160296,
        newRun.getOverallRollup().getOverview().get(StepInstance.Status.SUCCEEDED).getCnt());
    // prev rollup did not change after executing segregate
    assertEquals(160822, prev.getOverallRollup().getTotalLeafCount());

    newRun = new ForeachStepOverview();
    newRun.initiateStepRollup(null, null);
    assertEquals(0, newRun.getOverallRollup().getTotalLeafCount());

    newRun = new ForeachStepOverview();
    newRun.initiateStepRollup(new WorkflowRollupOverview(), new WorkflowRollupOverview());
    assertEquals(0, newRun.getOverallRollup().getTotalLeafCount());

    ForeachStepOverview newRun2 = new ForeachStepOverview();

    // trying to segregate 160k from 10 is invalid
    AssertHelper.assertThrows(
        "should throw exception for invalid case",
        IllegalArgumentException.class,
        "Rollup of the previous run cannot have less steps ",
        () -> newRun2.initiateStepRollup(aggregatedRollupsPrevRun, prev.getOverallRollup()));
  }

  @Test
  public void testGetIterationsToRunFromDetails() throws Exception {
    ForeachStepOverview prev =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    ForeachStepOverview curr =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    curr.getDetails().getInfo().remove(WorkflowInstance.Status.IN_PROGRESS);
    curr.getDetails().getInfo().remove(WorkflowInstance.Status.SUCCEEDED);
    curr.addOne(80110, WorkflowInstance.Status.SUCCEEDED, new WorkflowRollupOverview());
    curr.addOne(80115, WorkflowInstance.Status.SUCCEEDED, new WorkflowRollupOverview());
    curr.refreshDetail();

    List<Long> flattenedSuccPrevDetails =
        prev.getDetails().flatten(e -> true).get(WorkflowInstance.Status.SUCCEEDED);

    assertEquals(5, flattenedSuccPrevDetails.size());
    assertEquals(
        1, prev.getDetails().flatten(e -> true).get(WorkflowInstance.Status.IN_PROGRESS).size());
    assertTrue(flattenedSuccPrevDetails.contains(80110L));
    assertTrue(flattenedSuccPrevDetails.contains(80115L));
    assertTrue(flattenedSuccPrevDetails.contains(80112L));
    assertTrue(flattenedSuccPrevDetails.contains(80113L));
    assertTrue(flattenedSuccPrevDetails.contains(80114L));
    assertTrue(
        prev.getDetails()
            .flatten(e -> true)
            .get(WorkflowInstance.Status.IN_PROGRESS)
            .contains(70000L));

    Set<Long> result = curr.getIterationsToRunFromDetails(prev);
    assertEquals(4, result.size());
    assertFalse(result.contains(80110L));
    assertFalse(result.contains(80115L));

    ForeachStepOverview newOne = new ForeachStepOverview();
    result = newOne.getIterationsToRunFromDetails(prev);
    assertEquals(6, result.size());

    result = newOne.getIterationsToRunFromDetails(null);
    assertEquals(0, result.size());

    result = newOne.getIterationsToRunFromDetails(new ForeachStepOverview());
    assertEquals(0, result.size());

    result = prev.getIterationsToRunFromDetails(new ForeachStepOverview());
    assertEquals(0, result.size());
  }
}
