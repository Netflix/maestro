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
package com.netflix.maestro.engine.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.instance.RunConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RollupAggregationHelperTest extends MaestroEngineBaseTest {
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  private final RollupAggregationHelper rollupAggregationHelper;

  @Data
  private static class ArtifactMap {
    @JsonProperty("artifacts")
    private Map<String, Artifact> artifacts;
  }

  public RollupAggregationHelperTest() {
    this.rollupAggregationHelper = new RollupAggregationHelper(stepInstanceDao);
  }

  @Test
  public void testGetStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns() throws IOException {
    WorkflowInstance sampleInstance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created-foreach-subworkflow-1.json",
            WorkflowInstance.class);

    Map<String, Long> stepIdRunId =
        RollupAggregationHelper.getStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns(
            sampleInstance);

    // both subworkflow and foreach steps are included in the current run
    assertEquals(0, stepIdRunId.size());

    sampleInstance.getRuntimeDag().remove("job_subworkflow");

    stepIdRunId =
        RollupAggregationHelper.getStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns(
            sampleInstance);

    // foreach step is included in the current run
    // while subworkflow one is not
    assertEquals(1, stepIdRunId.size());
    assertEquals(3L, stepIdRunId.get("job_subworkflow").longValue());

    sampleInstance.getRuntimeDag().remove("job_foreach");

    stepIdRunId =
        RollupAggregationHelper.getStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns(
            sampleInstance);

    // both are included
    assertEquals(2, stepIdRunId.size());
    assertEquals(3L, stepIdRunId.get("job_subworkflow").longValue());
    assertEquals(1L, stepIdRunId.get("job_foreach").longValue());

    WorkflowInstance sampleInstanceNoForeachSubworkflow =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);

    stepIdRunId =
        RollupAggregationHelper.getStepIdToRunIdForLoopAndSubworkflowFromPreviousRuns(
            sampleInstanceNoForeachSubworkflow);

    assertEquals(0, stepIdRunId.size());
  }

  @Test
  public void testGetForeachAndSubworkflowStepRollups() throws IOException {
    ArtifactMap artifacts =
        loadObject("fixtures/artifact/sample-artifacts.json", ArtifactMap.class);

    artifacts.getArtifacts().remove("artifact1");
    artifacts.getArtifacts().remove("artifact2");

    String workflowId = "test_workflow_id";
    long workflowInstanceId = 2L;

    Map<String, Long> stepIdToRunId = new HashMap<>();
    stepIdToRunId.put("maestro_subworkflow", 1L);
    stepIdToRunId.put("maestro_foreach", 1L);

    doReturn(Collections.singletonList(artifacts.getArtifacts()))
        .when(stepInstanceDao)
        .getBatchStepInstancesArtifactsFromList(workflowId, workflowInstanceId, stepIdToRunId);

    RollupAggregationHelper rollupAggregationHelper = new RollupAggregationHelper(stepInstanceDao);

    List<WorkflowRollupOverview> rollups =
        rollupAggregationHelper.getLoopAndSubworkflowStepRollups(
            workflowId, workflowInstanceId, stepIdToRunId);

    assertEquals(3, rollups.size());
    assertEquals(29, rollups.get(0).getTotalLeafCount());
    assertEquals(2, rollups.get(0).getOverview().size());
    assertEquals(14, rollups.get(1).getTotalLeafCount());
    assertEquals(2, rollups.get(1).getOverview().size());
    assertEquals(5, rollups.get(2).getTotalLeafCount());
    assertEquals(1, rollups.get(2).getOverview().size());

    // passing null stepIdToRunId
    rollups =
        this.rollupAggregationHelper.getLoopAndSubworkflowStepRollups(
            workflowId, workflowInstanceId, null);
    assertTrue(rollups.isEmpty());

    // passing empty stepIdToRunId
    rollups =
        this.rollupAggregationHelper.getLoopAndSubworkflowStepRollups(
            workflowId, workflowInstanceId, new HashMap<>());
    assertTrue(rollups.isEmpty());
  }

  @Test
  public void testGetRollupsForStepsFromPreviousRuns() {
    Map<String, StepAggregatedView> stepMap = new HashMap<>();
    stepMap.put(
        "step1",
        StepAggregatedView.builder()
            .status(StepInstance.Status.SUCCEEDED)
            .workflowRunId(1L)
            .build());
    stepMap.put(
        "step2",
        StepAggregatedView.builder()
            .status(StepInstance.Status.FATALLY_FAILED)
            .workflowRunId(1L)
            .build());
    WorkflowInstanceAggregatedInfo aggregatedInfo = new WorkflowInstanceAggregatedInfo();
    aggregatedInfo.setStepAggregatedViews(stepMap);

    Map<String, StepTransition> runtimeDag = new LinkedHashMap<>();
    runtimeDag.put("step3", new StepTransition());
    runtimeDag.put("step4", new StepTransition());
    runtimeDag.put("step5", new StepTransition());

    Set<String> foreachAndSubworkflowStepIds = new HashSet<>(Collections.singletonList("step4"));

    List<WorkflowRollupOverview> rollups =
        RollupAggregationHelper.getRollupsForStepsFromPreviousRuns(
            "workflow_id", 1L, aggregatedInfo, runtimeDag, foreachAndSubworkflowStepIds);

    assertEquals(2, rollups.size());

    assertNotNull(rollups.get(0).getOverview().get(StepInstance.Status.FATALLY_FAILED));
    assertNotNull(rollups.get(1).getOverview().get(StepInstance.Status.SUCCEEDED));

    // empty aggregated info steps
    rollups =
        RollupAggregationHelper.getRollupsForStepsFromPreviousRuns(
            "workflow_id",
            1L,
            new WorkflowInstanceAggregatedInfo(),
            runtimeDag,
            foreachAndSubworkflowStepIds);

    assertEquals(0, rollups.size());

    // empty foreach and subworkflow steps list
    rollups =
        RollupAggregationHelper.getRollupsForStepsFromPreviousRuns(
            "workflow_id", 1L, aggregatedInfo, runtimeDag, new HashSet<>());

    assertEquals(2, rollups.size());

    // empty runtimeDAG
    rollups =
        RollupAggregationHelper.getRollupsForStepsFromPreviousRuns(
            "workflow_id", 1L, aggregatedInfo, new HashMap<>(), foreachAndSubworkflowStepIds);

    assertEquals(2, rollups.size());

    // scenario where some skipped because in runtimeDag, and some because of foreach or subworkflow
    stepMap.put(
        "step3",
        StepAggregatedView.builder()
            .status(StepInstance.Status.SUCCEEDED)
            .workflowRunId(1L)
            .build());
    stepMap.put(
        "step4",
        StepAggregatedView.builder()
            .status(StepInstance.Status.SUCCEEDED)
            .workflowRunId(1L)
            .build());
    stepMap.put(
        "step5",
        StepAggregatedView.builder()
            .status(StepInstance.Status.SUCCEEDED)
            .workflowRunId(1L)
            .build());
    stepMap.put(
        "step6",
        StepAggregatedView.builder()
            .status(StepInstance.Status.SUCCEEDED)
            .workflowRunId(1L)
            .build());
    stepMap.put(
        "step7",
        StepAggregatedView.builder().status(StepInstance.Status.STOPPED).workflowRunId(1L).build());

    foreachAndSubworkflowStepIds.add("step6");

    rollups =
        RollupAggregationHelper.getRollupsForStepsFromPreviousRuns(
            "workflow_id", 1L, aggregatedInfo, runtimeDag, foreachAndSubworkflowStepIds);

    assertEquals(3, rollups.size());
    assertNotNull(rollups.get(0).getOverview().get(StepInstance.Status.STOPPED));
  }

  @Test
  public void testCalculateBaseRollup() throws IOException {
    WorkflowInstance sampleInstance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created-foreach-subworkflow-1.json",
            WorkflowInstance.class);
    sampleInstance.setRunConfig(new RunConfig());
    sampleInstance.getRunConfig().setPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    WorkflowRollupOverview rollupBase = rollupAggregationHelper.calculateRollupBase(sampleInstance);
    assertNotNull(rollupBase.getOverview().get(StepInstance.Status.SUCCEEDED));
    assertEquals(1, rollupBase.getOverview().get(StepInstance.Status.SUCCEEDED).getCnt());

    rollupBase = rollupAggregationHelper.calculateRollupBase(null);
    assertNull(rollupBase);

    WorkflowInstance wi = new WorkflowInstance();
    rollupBase = rollupAggregationHelper.calculateRollupBase(wi);
    assertNull(rollupBase);
  }
}
