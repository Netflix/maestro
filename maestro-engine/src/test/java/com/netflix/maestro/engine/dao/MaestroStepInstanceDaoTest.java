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
package com.netflix.maestro.engine.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroTestHelper;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroDatabaseError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.artifact.SubworkflowArtifact;
import com.netflix.maestro.models.definition.TagList;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAttemptState;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.signal.SignalDependencies;
import com.netflix.maestro.models.signal.SignalOutputs;
import com.netflix.maestro.models.signal.SignalOutputsDefinition;
import com.netflix.maestro.models.signal.SignalTransformer;
import com.netflix.maestro.models.timeline.Timeline;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MaestroStepInstanceDaoTest extends MaestroDaoBaseTest {
  private static final String TEST_WORKFLOW_ID = "sample-dag-test-3";
  private static final String TEST_STEP_INSTANCE =
      "fixtures/instances/sample-step-instance-running.json";

  private static final String TEST_STEP_INSTANCE_SUBWORKFLOW =
      "fixtures/instances/sample-subworkflow-step-instance-running.json";

  private MaestroQueueSystem queueSystem;
  private MaestroStepInstanceDao stepDao;
  private StepInstance si;

  @Before
  public void setUp() throws Exception {
    queueSystem = Mockito.mock(MaestroQueueSystem.class);
    stepDao = new MaestroStepInstanceDao(dataSource, MAPPER, config, queueSystem, metricRepo);
    si = loadObject(TEST_STEP_INSTANCE, StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, false, null);
    verify(queueSystem, times(1)).notify(any());
    reset(queueSystem);
  }

  @After
  public void tearDown() {
    MaestroTestHelper.removeWorkflowInstance(dataSource, TEST_WORKFLOW_ID, 1);
    AssertHelper.assertThrows(
        "cannot get non-existing workflow instance",
        MaestroNotFoundException.class,
        "workflow instance [sample-dag-test-3][1][1]'s step instance [job1][1] not found",
        () -> stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1"));
  }

  @Test
  public void testInsertStepInstance() throws Exception {
    tearDown();
    stepDao.insertOrUpsertStepInstance(si, false, Mockito.mock(MaestroJobEvent.class));
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    StepInstance instance = stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertEquals(2, instance.getDefinition().getSignalOutputs().definitions().size());
    assertTrue(instance.getArtifacts().isEmpty());
    assertTrue(instance.getTimeline().isEmpty());
    instance.setArtifacts(null);
    instance.setTimeline(null);
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testInsertStepInstanceWithoutOutputSignalSummary() {
    tearDown();
    si.setSignalOutputs(null);
    stepDao.insertOrUpsertStepInstance(si, false, null);
    StepInstance instance = stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertNull(instance.getSignalOutputs());
    instance.setArtifacts(null);
    instance.setTimeline(null);
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testInsertDuplicateStepInstance() {
    AssertHelper.assertThrows(
        "cannot insert the same step instance twice",
        MaestroDatabaseError.class,
        "INTERNAL_ERROR - ERROR: duplicate key value",
        () -> stepDao.insertOrUpsertStepInstance(si, false, null));
  }

  @Test
  public void testUpsertStepInstance() {
    si.setArtifacts(Collections.emptyMap());
    si.setTimeline(new Timeline(Collections.emptyList()));
    stepDao.insertOrUpsertStepInstance(si, true, null);
    StepInstance instance = stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testUpdateStepInstance() throws Exception {
    si.getRuntimeState().setStatus(StepInstance.Status.SUCCEEDED);
    SignalOutputs outputs = new SignalOutputs();
    SignalOutputs.SignalOutput output =
        SignalTransformer.transform(
            new SignalOutputsDefinition.SignalOutputDefinition(),
            MapParameter.builder()
                .evaluatedResult(Collections.singletonMap("name", "signal_a"))
                .build());
    output.setSignalId(123L);
    output.setAnnounceTime(11122233445L);
    outputs.setOutputs(Collections.singletonList(output));
    si.setArtifacts(Collections.emptyMap());
    si.setSignalOutputs(outputs);
    si.setTimeline(new Timeline(Collections.emptyList()));
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId(TEST_WORKFLOW_ID);
    workflowSummary.setWorkflowInstanceId(1);
    workflowSummary.setWorkflowRunId(1);
    StepRuntimeSummary summary =
        StepRuntimeSummary.builder()
            .stepId("job1")
            .stepAttemptId(1)
            .stepInstanceId(1)
            .runtimeState(si.getRuntimeState())
            .artifacts(si.getArtifacts())
            .signalDependencies(si.getSignalDependencies())
            .signalOutputs(si.getSignalOutputs())
            .timeline(si.getTimeline())
            .build();
    stepDao.updateStepInstance(workflowSummary, summary, Mockito.mock(MaestroJobEvent.class));
    verify(queueSystem, times(1)).enqueue(any(), any());
    verify(queueSystem, times(1)).notify(any());
    StepInstance instance = stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertEquals(StepInstance.Status.SUCCEEDED, instance.getRuntimeState().getStatus());
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testGetStepInstance() {
    StepInstance instance = stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    StepInstance latest = stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(instance, latest);
    assertEquals(StepInstance.Status.RUNNING, instance.getRuntimeState().getStatus());
    assertFalse(instance.getSignalDependencies().isSatisfied());
    assertEquals(2, instance.getDefinition().getSignalOutputs().definitions().size());
    assertTrue(instance.getArtifacts().isEmpty());
    assertTrue(instance.getTimeline().isEmpty());
    instance.setArtifacts(null);
    instance.setTimeline(null);
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testGetStepInstanceWithInvalidAttempt() {
    AssertHelper.assertThrows(
        "cannot cast an invalid attempt id",
        NumberFormatException.class,
        "For input string: \"first\"",
        () -> stepDao.getStepInstance(TEST_WORKFLOW_ID, 1, 1, "job1", "first"));
  }

  @Test
  public void testGetStepInstanceRuntimeState() {
    StepRuntimeState state =
        stepDao.getStepInstanceRuntimeState(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertEquals(si.getRuntimeState(), state);
    StepRuntimeState latest =
        stepDao.getStepInstanceRuntimeState(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(state, latest);
  }

  @Test
  public void testGetStepInstanceArtifacts() {
    Map<String, Artifact> artifacts =
        stepDao.getStepInstanceArtifacts(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertTrue(artifacts.isEmpty());
    Map<String, Artifact> latest =
        stepDao.getStepInstanceArtifacts(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(artifacts, latest);
  }

  @Test
  public void testGetStepInstanceTags() {
    TagList tags = stepDao.getStepInstanceTags(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertEquals(si.getTags(), tags);
    TagList latest = stepDao.getStepInstanceTags(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(tags, latest);
  }

  @Test
  public void testGetStepInstanceTimeline() {
    Timeline timeline = stepDao.getStepInstanceTimeline(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertTrue(timeline.isEmpty());
    Timeline latest = stepDao.getStepInstanceTimeline(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(timeline, latest);
  }

  @Test
  public void testGetStepInstanceStepDependenciesSummary() {
    SignalDependencies signalDependencies =
        stepDao.getSignalDependencies(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertFalse(signalDependencies.isSatisfied());
    SignalDependencies latest =
        stepDao.getSignalDependencies(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(signalDependencies, latest);
  }

  @Test
  public void testGetStepInstanceOutputSignals() {
    SignalOutputs signals = stepDao.getSignalOutputs(TEST_WORKFLOW_ID, 1, 1, "job1", "1");
    assertEquals(2, signals.getOutputs().size());
    SignalOutputs latest = stepDao.getSignalOutputs(TEST_WORKFLOW_ID, 1, 1, "job1", "latest");
    assertEquals(signals, latest);
  }

  @Test
  public void testGetAllStepInstances() {
    List<StepInstance> instances = stepDao.getAllStepInstances(TEST_WORKFLOW_ID, 1, 1);
    assertEquals(1, instances.size());
    StepInstance instance = instances.getFirst();
    assertEquals(StepInstance.Status.RUNNING, instance.getRuntimeState().getStatus());
    assertFalse(instance.getSignalDependencies().isSatisfied());
    assertEquals(2, instance.getDefinition().getSignalOutputs().definitions().size());
    assertTrue(instance.getArtifacts().isEmpty());
    assertTrue(instance.getTimeline().isEmpty());
    instance.setArtifacts(null);
    instance.setTimeline(null);
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testGetStepInstances() {
    List<StepInstance> instances = stepDao.getStepInstances(TEST_WORKFLOW_ID, 1, 1, "job1");
    assertEquals(1, instances.size());
    StepInstance instance = instances.getFirst();
    assertEquals(StepInstance.Status.RUNNING, instance.getRuntimeState().getStatus());
    assertFalse(instance.getSignalDependencies().isSatisfied());
    assertEquals(2, instance.getSignalOutputs().getOutputs().size());
    assertTrue(instance.getArtifacts().isEmpty());
    assertTrue(instance.getTimeline().isEmpty());
    instance.setArtifacts(null);
    instance.setTimeline(null);
    Assertions.assertThat(instance).usingRecursiveComparison().isEqualTo(si);
  }

  @Test
  public void testGetAllStepStates() {
    Map<String, StepRuntimeState> stats = stepDao.getAllStepStates(TEST_WORKFLOW_ID, 1, 1);
    assertEquals(singletonMap(si.getStepId(), si.getRuntimeState()), stats);
  }

  @Test
  public void testGetStepStates() {
    Map<String, StepRuntimeState> stats =
        stepDao.getStepStates(TEST_WORKFLOW_ID, 1, 1, Arrays.asList("job1", "job2"));
    assertEquals(singletonMap(si.getStepId(), si.getRuntimeState()), stats);
  }

  @Test
  public void testGetAllLatestStepStatusFromAncestors() throws Exception {
    si = loadObject("fixtures/instances/sample-step-instance-finishing.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    si = loadObject("fixtures/instances/sample-step-instance-failed.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    Map<String, StepInstance.Status> res =
        stepDao.getAllLatestStepStatusFromRuns("sample-dag-test-3", 1L);
    assertEquals(Collections.singletonMap("job1", StepInstance.Status.FINISHING), res);
  }

  @Test
  public void testGetAllLatestStepFromAncestors() throws Exception {
    si = loadObject("fixtures/instances/sample-step-instance-finishing.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    StepInstance expected = si;
    expected.setArtifacts(Collections.emptyMap());
    si = loadObject("fixtures/instances/sample-step-instance-failed.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);

    Map<String, StepInstance> res =
        stepDao.getAllLatestStepFromAncestors("sample-dag-test-3", 1L, List.of("job1"));
    assertEquals(Collections.singletonMap("job1", expected), res);
  }

  @Test
  public void testGetLatestSubworkflowArtifactForRuns() throws Exception {
    SubworkflowArtifact artifact =
        stepDao.getLatestSubworkflowArtifact("sample-subworkflow-wf", 1L, "job1");
    assertNull(artifact);
    si =
        loadObject(
            "fixtures/instances/sample-subworkflow-step-instance-running.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    artifact = stepDao.getLatestSubworkflowArtifact("sample-subworkflow-wf", 1L, "job1");
    assertEquals("sample-dag-test-3", artifact.getSubworkflowId());
    assertEquals(1L, artifact.getSubworkflowVersionId());
    assertEquals(2L, artifact.getSubworkflowInstanceId());
    assertEquals(4L, artifact.getSubworkflowRunId());
    assertEquals("39e7bf75-d5b0-390a-a681-46707c6293aa", artifact.getSubworkflowUuid());
    assertEquals(
        WorkflowRuntimeOverview.of(
            1L,
            singletonEnumMap(StepInstance.Status.RUNNING, WorkflowStepStatusSummary.of(1L)),
            null),
        artifact.getSubworkflowOverview());
  }

  @Test
  public void testGetLatestForeachArtifactForRuns() throws Exception {
    ForeachArtifact artifact =
        stepDao.getLatestForeachArtifact("sample-foreach-wf", 1L, "foreach-step1");
    assertNull(artifact);
    si =
        loadObject(
            "fixtures/instances/sample-foreach-step-instance-running.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    artifact = stepDao.getLatestForeachArtifact("sample-foreach-wf", 1L, "foreach-step1");
    assertEquals(
        "maestro_foreach_Ib2_11_94587073c5c260cfd048a0d09251a917", artifact.getForeachWorkflowId());
    assertEquals(5L, artifact.getForeachRunId());
    assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, artifact.getRunPolicy());
    assertEquals(6L, artifact.getAncestorIterationCount().longValue());
    assertEquals(6, artifact.getNextLoopIndex());
    assertEquals(si.getArtifacts().get("maestro_foreach"), artifact);
  }

  @Test
  public void testGetStepInstanceView() throws Exception {
    si = loadObject("fixtures/instances/sample-step-instance-finishing.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    si = loadObject("fixtures/instances/sample-step-instance-failed.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    StepInstance instance = stepDao.getStepInstanceView("sample-dag-test-3", 1L, "job1");
    assertEquals(2L, instance.getWorkflowRunId());
    assertEquals(2L, instance.getStepAttemptId());
    assertEquals(StepInstance.Status.FINISHING, instance.getRuntimeState().getStatus());
  }

  @Test
  public void testGetStepAttemptStateView() throws Exception {
    // sample-step-instance-running.json is inserted as part of the setup, before this test

    si = loadObject("fixtures/instances/sample-step-instance-finishing.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    si = loadObject("fixtures/instances/sample-step-instance-failed.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si, true, null);
    List<StepAttemptState> instances =
        stepDao.getStepAttemptStates("sample-dag-test-3", 1L, "job1");

    assertEquals(3, instances.size());

    StepAttemptState run2Att2 = instances.get(0);
    StepAttemptState run2Att1 = instances.get(1);
    StepAttemptState run1Att1 = instances.get(2);

    for (StepAttemptState stepAttemptState : instances) {
      assertEquals("sample-dag-test-3", stepAttemptState.getWorkflowId());
      assertEquals("job1", stepAttemptState.getStepId());
      assertEquals(1, stepAttemptState.getWorkflowInstanceId());
      assertEquals(1, stepAttemptState.getWorkflowVersionId());
    }

    assertEquals(2, run2Att2.getWorkflowRunId());
    assertEquals(2, run2Att2.getStepAttemptId());
    assertEquals(StepInstance.Status.FINISHING, run2Att2.getRuntimeState().getStatus());

    assertEquals(2, run2Att1.getWorkflowRunId());
    assertEquals(1, run2Att1.getStepAttemptId());
    assertEquals(StepInstance.Status.FATALLY_FAILED, run2Att1.getRuntimeState().getStatus());

    assertEquals(1, run1Att1.getWorkflowRunId());
    assertEquals(1, run1Att1.getStepAttemptId());
    assertEquals(StepInstance.Status.RUNNING, run1Att1.getRuntimeState().getStatus());
  }

  @Test
  public void testGetForeachParamType() throws Exception {
    si = loadObject("fixtures/instances/sample-step-instance-succeeded.json", StepInstance.class);
    si.setStepAttemptId(10);
    stepDao.insertOrUpsertStepInstance(si, false, null);
    assertEquals(
        ParamType.LONG, stepDao.getForeachParamType("sample-dag-test-3", "job1", "sleep_seconds"));
  }

  @Test
  public void testGetEvaluatedResultsFromForeach() throws Exception {
    si = loadObject("fixtures/instances/sample-step-instance-succeeded.json", StepInstance.class);
    si.setStepAttemptId(10);
    stepDao.insertOrUpsertStepInstance(si, false, null);
    assertEquals(
        Collections.singletonMap(1L, "15"),
        stepDao.getEvaluatedResultsFromForeach("sample-dag-test-3", "job1", "sleep_seconds"));
  }

  @Test
  public void testParamExtensionSqlInjection() throws Exception {
    si = loadObject("fixtures/instances/sample-step-instance-succeeded.json", StepInstance.class);
    si.setStepAttemptId(10);
    stepDao.insertOrUpsertStepInstance(si, false, null);
    AssertHelper.assertThrows(
        "sql injection won't work",
        MaestroNotFoundException.class,
        "not found (either not created or deleted)",
        () ->
            stepDao.getForeachParamType(
                "sample-dag-test-3",
                "job1",
                "sleep_seconds' FROM maestro_step_instance WHERE workflow_id=? and step_id=? limit 1;"
                    + " DELETE from maestro_step_instance; --"));

    assertEquals(
        Collections.emptyMap(),
        stepDao.getEvaluatedResultsFromForeach(
            "sample-dag-test-3",
            "job1",
            "sleep_seconds' FROM maestro_step_instance WHERE workflow_id=? and step_id=? limit 1;"
                + " DELETE from maestro_step_instance; --"));

    assertEquals(
        Collections.singletonMap(1L, "15"),
        stepDao.getEvaluatedResultsFromForeach("sample-dag-test-3", "job1", "sleep_seconds"));
  }

  @Test
  public void testGetNextUniqueId() {
    assertNotNull(stepDao.getNextUniqueId());
  }

  @Test
  public void testGetBatchStepInstancesArtifactsFromList() throws IOException {
    MaestroStepInstanceDao stepDaoSpy = Mockito.spy(stepDao);
    StepInstance siSubWf = loadObject(TEST_STEP_INSTANCE_SUBWORKFLOW, StepInstance.class);
    Map<String, Long> stepIdToRunId = new LinkedHashMap<>();
    int numberOfInstancesToInsert = Constants.BATCH_SIZE_ROLLUP_STEP_ARTIFACTS_QUERY * 2 + 3;
    long runId = 1;
    for (int i = 1; i <= numberOfInstancesToInsert; i++) {
      siSubWf.setStepId("step_" + i);
      siSubWf.setWorkflowRunId(runId);
      stepIdToRunId.put("step_" + i, runId);
      stepDaoSpy.insertOrUpsertStepInstance(siSubWf, false, null);
      if (i == numberOfInstancesToInsert / 2) {
        runId = 2;
      }
    }

    List<Map<String, Artifact>> artifacts =
        stepDaoSpy.getBatchStepInstancesArtifactsFromList(
            siSubWf.getWorkflowId(), siSubWf.getWorkflowInstanceId(), stepIdToRunId);

    assertEquals(numberOfInstancesToInsert, artifacts.size());

    assertEquals(
        "sample-dag-test-3",
        artifacts.get(10).get("maestro_subworkflow").asSubworkflow().getSubworkflowId());
    assertNotNull(
        artifacts.get(10).get("maestro_subworkflow").asSubworkflow().getSubworkflowOverview());

    Mockito.verify(stepDaoSpy, Mockito.times(3))
        .getBatchStepInstancesArtifactsFromListLimited(
            eq(siSubWf.getWorkflowId()), eq(siSubWf.getWorkflowInstanceId()), any());
  }

  @Test
  public void testGetBatchStepInstancesArtifactsFromList1Element() throws IOException {
    MaestroStepInstanceDao stepDaoSpy = Mockito.spy(stepDao);
    StepInstance siSubWf = loadObject(TEST_STEP_INSTANCE_SUBWORKFLOW, StepInstance.class);
    Map<String, Long> stepIdToRunId = new LinkedHashMap<>();
    siSubWf.setStepId("step_100");
    siSubWf.setWorkflowRunId(1);
    stepIdToRunId.put("step_100", 1L);
    stepDaoSpy.insertOrUpsertStepInstance(siSubWf, false, null);

    List<Map<String, Artifact>> artifacts =
        stepDaoSpy.getBatchStepInstancesArtifactsFromList(
            siSubWf.getWorkflowId(), siSubWf.getWorkflowInstanceId(), stepIdToRunId);

    assertEquals(1, artifacts.size());
    assertEquals(siSubWf.getArtifacts(), artifacts.getFirst());
    Mockito.verify(stepDaoSpy, Mockito.times(1))
        .getBatchStepInstancesArtifactsFromListLimited(
            eq(siSubWf.getWorkflowId()), eq(siSubWf.getWorkflowInstanceId()), any());
  }

  @Test
  public void testGetBatchStepInstancesArtifactsFromListStepNonExistent() throws IOException {
    MaestroStepInstanceDao stepDaoSpy = Mockito.spy(stepDao);
    StepInstance siSubWf = loadObject(TEST_STEP_INSTANCE_SUBWORKFLOW, StepInstance.class);
    Map<String, Long> stepIdToRunId = new LinkedHashMap<>();
    stepIdToRunId.put("step_113154651215", 1L);

    AssertHelper.assertThrows(
        "step doesn't exist",
        MaestroNotFoundException.class,
        "not found (either empty, not created, or deleted)",
        () ->
            stepDaoSpy.getBatchStepInstancesArtifactsFromList(
                siSubWf.getWorkflowId(), siSubWf.getWorkflowInstanceId(), stepIdToRunId));
  }

  @Test
  public void testGetBatchStepInstancesArtifactsFromListNullArtifactsFor1Step() throws IOException {
    MaestroStepInstanceDao stepDaoSpy = Mockito.spy(stepDao);
    StepInstance siSubWf = loadObject(TEST_STEP_INSTANCE_SUBWORKFLOW, StepInstance.class);
    Map<String, Long> stepIdToRunId = new LinkedHashMap<>();
    int numberOfInstancesToInsert = 3;
    long runId = 1;
    for (int i = 1; i <= numberOfInstancesToInsert; i++) {
      siSubWf.setStepId("step_for_null_test_" + i);
      siSubWf.setWorkflowRunId(runId);
      stepIdToRunId.put("step_for_null_test_" + i, runId);
      stepDaoSpy.insertOrUpsertStepInstance(siSubWf, false, null);
    }

    siSubWf.setStepId("step_for_null_test_" + numberOfInstancesToInsert + 1);
    siSubWf.setArtifacts(null);
    stepDaoSpy.insertOrUpsertStepInstance(siSubWf, false, null);
    stepIdToRunId.put("step_for_null_test_" + numberOfInstancesToInsert + 1, runId);

    List<Map<String, Artifact>> artifacts =
        stepDaoSpy.getBatchStepInstancesArtifactsFromList(
            siSubWf.getWorkflowId(), siSubWf.getWorkflowInstanceId(), stepIdToRunId);

    // still 4 artifacts, one of them is empty
    assertEquals(4, artifacts.size());
  }

  @Test
  public void testGetStepInstanceViews() throws Exception {
    StepInstance si1 =
        loadObject("fixtures/instances/sample-step-instance-finishing.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si1, true, null);
    StepInstance si2 =
        loadObject("fixtures/instances/sample-step-instance-failed.json", StepInstance.class);
    stepDao.insertOrUpsertStepInstance(si2, true, null);

    List<StepInstance> res = stepDao.getStepInstanceViews("sample-dag-test-3", 1L, 1L);
    assertEquals(1, res.size());
    assertEquals("ff4ccce2-0fda-4882-9cd8-12ff90cb5f06", res.getFirst().getStepUuid());

    res = stepDao.getStepInstanceViews("sample-dag-test-3", 1L, 2L);
    assertEquals(1, res.size());
    assertEquals("ff4ccce2-0fda-4882-9cd8-12ff90cb5f02", res.getFirst().getStepUuid());

    res = stepDao.getStepInstanceViews("sample-dag-test-3", 1L, 3L);
    assertEquals(0, res.size());
  }
}
