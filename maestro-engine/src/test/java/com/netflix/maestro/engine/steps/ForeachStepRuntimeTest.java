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
package com.netflix.maestro.engine.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.properties.ForeachStepRuntimeProperties;
import com.netflix.maestro.models.Actions.StepInstanceAction;
import com.netflix.maestro.models.artifact.Artifact.Type;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.ForeachStepOverview;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepInstance.StepRetry;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.queue.MaestroQueueSystem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ForeachStepRuntimeTest extends MaestroEngineBaseTest {
  private static final String WORKFLOW_ID = "fake_workflow_id";
  private static final long INSTANCE_ID = 1L;
  private static final long RUN_ID = 1L;
  private static final String STEP_ID = "fake_step_id";
  private static final long STEP_ATTEMPT_ID = 1L;

  private ForeachStepRuntime foreachStepRuntime;
  @Mock private WorkflowActionHandler workflowActionHandler;
  private MaestroWorkflowInstanceDao workflowInstanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroStepInstanceActionDao stepInstanceActionDao;
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private InstanceStepConcurrencyHandler instanceStepConcurrencyHandler;
  @Mock private ForeachStepRuntimeProperties foreachProperties;
  private WorkflowSummary workflowSummary;

  @Before
  public void setup() {
    workflowInstanceDao = Mockito.mock(MaestroWorkflowInstanceDao.class);
    foreachStepRuntime =
        new ForeachStepRuntime(
            workflowActionHandler,
            workflowInstanceDao,
            stepInstanceDao,
            stepInstanceActionDao,
            queueSystem,
            instanceStepConcurrencyHandler,
            foreachProperties);
    doReturn(5).when(foreachProperties).getGetRollupBatchLimit();

    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId(WORKFLOW_ID);
    workflowSummary.setWorkflowInstanceId(INSTANCE_ID);
    workflowSummary.setWorkflowRunId(RUN_ID);
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("test-workflow");
    parent.setInstanceId(12);
    parent.setRunId(2);
    parent.setStepId("foreach-step");
    parent.setStepAttemptId(1);
    initiator.setAncestors(Collections.singletonList(parent));
    workflowSummary.setInitiator(initiator);
    workflowSummary.setInternalId(12345L);
  }

  @Test
  public void testGetAggregatedRollupFromIterationsManyUneven() {
    ArgumentCaptor<List<Long>> captor = ArgumentCaptor.forClass(List.class);

    Set<Long> iterations = LongStream.rangeClosed(1, 23).boxed().collect(Collectors.toSet());
    doReturn(Collections.singletonList(new WorkflowRollupOverview()))
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(anyString(), any());

    ForeachStepOverview stepOverview = mock(ForeachStepOverview.class);
    ForeachStepOverview prevStepOverview = new ForeachStepOverview();
    doReturn(iterations).when(stepOverview).getIterationsToRunFromDetails(any());
    foreachStepRuntime.initializeForeachArtifactRollup(
        stepOverview, prevStepOverview, "myworkflowid");
    Mockito.verify(workflowInstanceDao, times(5))
        .getBatchForeachLatestRunRollupForIterations(eq("myworkflowid"), captor.capture());
    List<List<Long>> values = captor.getAllValues();
    long i = 1;
    for (List<Long> list : values) {
      if (i == 21) {
        assertEquals(3, list.size());
      } else {
        assertEquals(5, list.size());
      }
      for (Long val : list) {
        assertEquals(i++, val.longValue());
      }
    }
    assertEquals(24, i);
  }

  @Test
  public void testGetAggregatedRollupFromIterationsManyEven() {
    ArgumentCaptor<List<Long>> captor = ArgumentCaptor.forClass(List.class);

    Set<Long> iterations = LongStream.rangeClosed(1, 10).boxed().collect(Collectors.toSet());
    doReturn(Collections.singletonList(new WorkflowRollupOverview()))
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(anyString(), any());
    ForeachStepOverview stepOverview = mock(ForeachStepOverview.class);
    ForeachStepOverview prevStepOverview = new ForeachStepOverview();
    doReturn(iterations).when(stepOverview).getIterationsToRunFromDetails(any());
    foreachStepRuntime.initializeForeachArtifactRollup(
        stepOverview, prevStepOverview, "myworkflowid");
    Mockito.verify(workflowInstanceDao, times(2))
        .getBatchForeachLatestRunRollupForIterations(eq("myworkflowid"), captor.capture());
    List<List<Long>> values = captor.getAllValues();
    assertEquals(5, values.get(0).size());
    assertEquals(5, values.get(1).size());
  }

  @Test
  public void testGetAggregatedRollupFromIterationsNotManyUneven() {
    ArgumentCaptor<List<Long>> captor = ArgumentCaptor.forClass(List.class);

    Set<Long> iterations = LongStream.rangeClosed(1, 3).boxed().collect(Collectors.toSet());
    doReturn(Collections.singletonList(new WorkflowRollupOverview()))
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(anyString(), any());
    ForeachStepOverview stepOverview = mock(ForeachStepOverview.class);
    ForeachStepOverview prevStepOverview = new ForeachStepOverview();
    doReturn(iterations).when(stepOverview).getIterationsToRunFromDetails(any());
    foreachStepRuntime.initializeForeachArtifactRollup(
        stepOverview, prevStepOverview, "myworkflowid");
    Mockito.verify(workflowInstanceDao, times(1))
        .getBatchForeachLatestRunRollupForIterations(eq("myworkflowid"), captor.capture());
    List<List<Long>> values = captor.getAllValues();
    assertEquals(1, values.get(0).get(0).longValue());
    assertEquals(2, values.get(0).get(1).longValue());
    assertEquals(3, values.get(0).get(2).longValue());
    assertEquals(3, values.get(0).size());
  }

  @Test
  public void testGetAggregatedRollupFromIterationsNull() {
    doReturn(Collections.singletonList(new WorkflowRollupOverview()))
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(anyString(), any());
    ForeachStepOverview stepOverview = mock(ForeachStepOverview.class);
    ForeachStepOverview prevStepOverview = new ForeachStepOverview();
    doReturn(null).when(stepOverview).getIterationsToRunFromDetails(any());
    foreachStepRuntime.initializeForeachArtifactRollup(
        stepOverview, prevStepOverview, "myworkflowid");
    assertNull(stepOverview.getRollup());
    Mockito.verify(workflowInstanceDao, times(0))
        .getBatchForeachLatestRunRollupForIterations(eq("myworkflowid"), any());
  }

  @Test
  public void testGetAggregatedRollupFromIterationsEmpty() {
    doReturn(Collections.singletonList(new WorkflowRollupOverview()))
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(anyString(), any());
    ForeachStepOverview stepOverview = mock(ForeachStepOverview.class);
    ForeachStepOverview prevStepOverview = new ForeachStepOverview();
    doReturn(new HashSet<Long>()).when(stepOverview).getIterationsToRunFromDetails(any());
    foreachStepRuntime.initializeForeachArtifactRollup(
        stepOverview, prevStepOverview, "myworkflowid");
    assertNull(stepOverview.getRollup());
    Mockito.verify(workflowInstanceDao, times(0))
        .getBatchForeachLatestRunRollupForIterations(eq("myworkflowid"), any());
  }

  @Test
  public void testGetAggregatedRollupFromIterationsStepEmpty() {
    Set<Long> iterations = LongStream.rangeClosed(1, 3).boxed().collect(Collectors.toSet());
    doReturn(Collections.singletonList(new WorkflowRollupOverview()))
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(anyString(), any());
    ForeachStepOverview stepOverview = mock(ForeachStepOverview.class);
    ForeachStepOverview prevStepOverview = new ForeachStepOverview();
    doReturn(iterations).when(stepOverview).getIterationsToRunFromDetails(any());
    foreachStepRuntime.initializeForeachArtifactRollup(stepOverview, prevStepOverview, "");
    assertNull(stepOverview.getRollup());
    Mockito.verify(workflowInstanceDao, times(0))
        .getBatchForeachLatestRunRollupForIterations(eq(""), any());
  }

  @Test
  public void testInitializeForeachArtifactRollup() throws IOException {
    ForeachStepOverview curr =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    ForeachStepOverview prev =
        loadObject(
            "fixtures/instances/sample-foreach-step-overview.json", ForeachStepOverview.class);

    curr.getDetails().getInfo().remove(WorkflowInstance.Status.IN_PROGRESS);
    curr.getDetails().getInfo().remove(WorkflowInstance.Status.SUCCEEDED);
    curr.addOne(80110, WorkflowInstance.Status.SUCCEEDED, new WorkflowRollupOverview());
    curr.addOne(80115, WorkflowInstance.Status.SUCCEEDED, new WorkflowRollupOverview());
    curr.refreshDetail();
    curr.setRollup(null);

    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(10);
    WorkflowRollupOverview.CountReference ref2 = new WorkflowRollupOverview.CountReference();
    ref2.setCnt(2);
    WorkflowRollupOverview rollup = new WorkflowRollupOverview();
    rollup.setTotalLeafCount(12);
    EnumMap<StepInstance.Status, WorkflowRollupOverview.CountReference> overview =
        new EnumMap<>(StepInstance.Status.class);
    overview.put(StepInstance.Status.SUCCEEDED, ref);
    overview.put(StepInstance.Status.FATALLY_FAILED, ref2);
    rollup.setOverview(overview);

    prev.setRollup(rollup);

    List<WorkflowRollupOverview> rollups = new ArrayList<>();
    ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    rollup = new WorkflowRollupOverview();
    rollup.setTotalLeafCount(1);
    rollup.setOverview(singletonEnumMap(StepInstance.Status.FATALLY_FAILED, ref));
    rollups.add(rollup);
    rollups.add(rollup);

    doReturn(rollups)
        .when(workflowInstanceDao)
        .getBatchForeachLatestRunRollupForIterations(eq("workflowid"), any());
    foreachStepRuntime.initializeForeachArtifactRollup(curr, prev, "workflowid");
    assertEquals(10, curr.getRollup().getTotalLeafCount());
    assertEquals(10, curr.getRollup().getOverview().get(StepInstance.Status.SUCCEEDED).getCnt());
  }

  @Test
  public void testForeachCreateArtifactWithRestartFromSpecificNotAlongRestartPath() {
    int restartIterationId = 2;
    Map<String, Object> evaluatedResult = new LinkedHashMap<>();
    evaluatedResult.put("loop_param", new long[] {1, 2, 3});

    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put(
        "loop_params",
        MapParameter.builder()
            .evaluatedResult(evaluatedResult)
            .evaluatedTime(System.currentTimeMillis())
            .build());

    StepRetry stepRetry = new StepRetry();
    stepRetry.setRetryable(false);
    RestartConfig restartConfig =
        RestartConfig.builder()
            .addRestartNode("maestro-foreach-wf", restartIterationId, "job2")
            .addRestartNode("wf", 1, "job1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .build();
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId(STEP_ID)
            .stepAttemptId(STEP_ATTEMPT_ID)
            .artifacts(new HashMap<>())
            .params(params)
            .stepRetry(stepRetry)
            .restartConfig(restartConfig)
            .build();

    ForeachArtifact prevArtifact = new ForeachArtifact();
    prevArtifact.setForeachWorkflowId("maestro-foreach-wf");
    prevArtifact.setNextLoopIndex(1);
    prevArtifact.setForeachOverview(new ForeachStepOverview());
    prevArtifact.getForeachOverview().addOne(1, WorkflowInstance.Status.FAILED, null);
    prevArtifact.getForeachOverview().refreshDetail();

    Mockito.when(stepInstanceDao.getLatestForeachArtifact(anyString(), anyLong(), anyString()))
        .thenReturn(prevArtifact);

    StepRuntime.Result res = foreachStepRuntime.start(workflowSummary, null, runtimeSummary);
    ForeachArtifact artifact = res.artifacts().get(Type.FOREACH.key()).asForeach();
    assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, artifact.getRunPolicy());
    assertNull(artifact.getPendingAction());
  }

  @Test
  public void testForeachCreateArtifactWithRestartFromSpecificAlongRestartPath() {
    int restartIterationId = 2;
    Map<String, Object> evaluatedResult = new LinkedHashMap<>();
    evaluatedResult.put("loop_param", new long[] {1, 2, 3});

    Map<String, Parameter> params = new LinkedHashMap<>();
    params.put(
        "loop_params",
        MapParameter.builder()
            .evaluatedResult(evaluatedResult)
            .evaluatedTime(System.currentTimeMillis())
            .build());

    StepRetry stepRetry = new StepRetry();
    stepRetry.setRetryable(false);
    RestartConfig restartConfig =
        RestartConfig.builder()
            .addRestartNode(
                "maestro_foreach_7D3_1C_2fe596608afaef7a41b4ec3c0edc3973",
                restartIterationId,
                "job2")
            .addRestartNode("wf", 1, "job1")
            .restartPolicy(RunPolicy.RESTART_FROM_SPECIFIC)
            .build();
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId(STEP_ID)
            .stepAttemptId(STEP_ATTEMPT_ID)
            .artifacts(new HashMap<>())
            .params(params)
            .stepRetry(stepRetry)
            .restartConfig(restartConfig)
            .build();

    ForeachArtifact prevArtifact = new ForeachArtifact();
    prevArtifact.setForeachWorkflowId("maestro-foreach-wf");
    prevArtifact.setNextLoopIndex(1);
    prevArtifact.setForeachOverview(new ForeachStepOverview());
    prevArtifact.getForeachOverview().addOne(1, WorkflowInstance.Status.FAILED, null);
    prevArtifact.getForeachOverview().refreshDetail();

    Mockito.when(stepInstanceDao.getLatestForeachArtifact(anyString(), anyLong(), anyString()))
        .thenReturn(prevArtifact);

    StepRuntime.Result res = foreachStepRuntime.start(workflowSummary, null, runtimeSummary);
    ForeachArtifact artifact = res.artifacts().get(Type.FOREACH.key()).asForeach();
    assertEquals(RunPolicy.RESTART_FROM_SPECIFIC, artifact.getRunPolicy());
    assertEquals(restartConfig, artifact.getPendingAction().getRestartConfig());
    assertEquals(StepInstanceAction.RESTART, artifact.getPendingAction().getAction());
    assertEquals(2L, artifact.getPendingAction().getInstanceId());
    assertEquals(0L, artifact.getPendingAction().getInstanceRunId());
    assertEquals(User.create("maestro"), artifact.getPendingAction().getUser());
  }
}
