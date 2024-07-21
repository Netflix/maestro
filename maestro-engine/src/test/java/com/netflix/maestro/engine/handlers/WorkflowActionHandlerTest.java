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
package com.netflix.maestro.engine.handlers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.jobevents.StartWorkflowJobEvent;
import com.netflix.maestro.engine.jobevents.WorkflowVersionUpdateJobEvent;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.engine.validations.DryRunValidator;
import com.netflix.maestro.exceptions.InvalidWorkflowVersionException;
import com.netflix.maestro.exceptions.MaestroResourceConflictException;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.api.WorkflowActionResponse;
import com.netflix.maestro.models.api.WorkflowCreateRequest;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.PropertiesSnapshot;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.SignalInitiator;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.ForeachStepOverview;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.trigger.TriggerUuids;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

public class WorkflowActionHandlerTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowDao workflowDao;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroRunStrategyDao runStrategyDao;
  @Mock private DagTranslator dagTranslator;
  @Mock private WorkflowRunner runner;
  @Mock private ParamEvaluator evaluator;
  @Mock private DryRunValidator dryRunValidator;
  @Mock private MaestroParamExtensionRepo extensionRepo;
  @Mock private MaestroJobEventPublisher maestroJobEventPublisher;

  private WorkflowDefinition definition;
  private WorkflowInstance instance;
  private WorkflowActionHandler actionHandler;
  private WorkflowHelper workflowHelper;
  private final User tester = User.create("tester");

  @Before
  public void before() throws Exception {
    ParamsManager paramsManager = mock(ParamsManager.class);
    this.workflowHelper =
        spy(
            new WorkflowHelper(
                paramsManager, evaluator, dagTranslator, extensionRepo, maestroJobEventPublisher));
    this.actionHandler =
        new WorkflowActionHandler(
            workflowDao, instanceDao, runStrategyDao, dryRunValidator, this.workflowHelper);
    definition =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf.json", WorkflowDefinition.class);
    definition.getMetadata().setWorkflowVersionId(1L);
    instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId("sample-minimal-wf");
    when(workflowDao.getWorkflowDefinition("sample-minimal-wf", "active")).thenReturn(definition);
    when(instanceDao.runWorkflowInstances(any(), any(), anyInt())).thenReturn(Optional.empty());
    when(runner.start(any())).thenReturn("test-uuid");
  }

  @Test
  public void testWorkflowValidate() {
    WorkflowCreateRequest request = new WorkflowCreateRequest();
    request.setWorkflow(definition.getWorkflow());
    actionHandler.validate(request, tester);
    verify(dryRunValidator, times(1)).validate(any(), any());
    verify(workflowDao, times(0)).addWorkflowDefinition(any(), any());
  }

  @Test
  public void testRunStrategy() throws IOException {
    WorkflowDefinition definitionParallelRunStrategy =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf-run-strategy-parallel.json",
            WorkflowDefinition.class);
    definitionParallelRunStrategy.getMetadata().setWorkflowVersionId(1L);
    WorkflowInstance instanceParallelRunStrategy =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instanceParallelRunStrategy.setWorkflowId("sample-minimal-wf-run-strategy-parallel");
    when(workflowDao.getWorkflowDefinition("sample-minimal-wf-run-strategy-parallel", "active"))
        .thenReturn(definitionParallelRunStrategy);
    AtomicReference<WorkflowInstance> instanceRef = new AtomicReference<>();
    doAnswer(
            (Answer<Integer>)
                invocation -> {
                  WorkflowInstance instance = (WorkflowInstance) invocation.getArguments()[0];
                  assertEquals(0L, instance.getWorkflowInstanceId());
                  assertEquals(0L, instance.getWorkflowRunId());
                  instance.setWorkflowInstanceId(1L);
                  instance.setWorkflowRunId(1L);
                  instanceRef.set(instance);
                  return 1;
                })
        .when(runStrategyDao)
        .startWithRunStrategy(any(), any());
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .build();
    actionHandler.start("sample-minimal-wf", "active", request);
    verify(runStrategyDao, times(1))
        .startWithRunStrategy(instanceRef.get(), Defaults.DEFAULT_RUN_STRATEGY);
    actionHandler.start("sample-minimal-wf-run-strategy-parallel", "active", request);
    assertEquals(
        RunStrategy.Rule.PARALLEL,
        definitionParallelRunStrategy.getRunStrategyOrDefault().getRule());
    verify(runStrategyDao, times(1))
        .startWithRunStrategy(
            instanceRef.get(), definitionParallelRunStrategy.getRunStrategyOrDefault());
  }

  @Test
  public void testStartCreated() {
    doAnswer(
            (Answer<Integer>)
                invocation -> {
                  WorkflowInstance instance = (WorkflowInstance) invocation.getArguments()[0];
                  assertEquals(0L, instance.getWorkflowInstanceId());
                  assertEquals(0L, instance.getWorkflowRunId());
                  instance.setWorkflowInstanceId(1L);
                  instance.setWorkflowRunId(1L);
                  return 1;
                })
        .when(runStrategyDao)
        .startWithRunStrategy(any(), any());
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .build();
    RunResponse response = actionHandler.start("sample-minimal-wf", "active", request);
    verify(workflowDao, times(1)).getWorkflowDefinition("sample-minimal-wf", "active");
    verify(runStrategyDao, times(1)).startWithRunStrategy(any(), any());
    verify(dagTranslator, times(1)).translate(any());
    assertEquals(instance.getWorkflowId(), response.getWorkflowId());
    assertEquals(1L, response.getWorkflowVersionId());
    assertEquals(1L, response.getWorkflowInstanceId());
    assertEquals(1L, response.getWorkflowRunId());
    assertEquals("41f0281e-41a2-468d-b830-56141b2f768b", response.getWorkflowUuid());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
  }

  @Test
  public void testStartWithInvalidStepRunParams() {
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .stepRunParams(Collections.singletonMap("job1", Collections.emptyMap()))
            .build();
    AssertHelper.assertThrows(
        "caller cannot be null to activate workflow",
        IllegalArgumentException.class,
        "non-existing step id detected in step param overrides: inputs [job1] vs dag",
        () -> actionHandler.start("sample-minimal-wf", "active", request));

    verify(workflowDao, times(1)).getWorkflowDefinition("sample-minimal-wf", "active");
    verify(dagTranslator, times(1)).translate(any());
  }

  @Test
  public void testStartSubworkflow() {
    doAnswer(
            (Answer<Integer>)
                invocation -> {
                  WorkflowInstance instance = (WorkflowInstance) invocation.getArguments()[0];
                  assertEquals(0L, instance.getWorkflowInstanceId());
                  assertEquals(0L, instance.getWorkflowRunId());
                  instance.setWorkflowInstanceId(1L);
                  instance.setWorkflowRunId(10L);
                  return 1;
                })
        .when(runStrategyDao)
        .startWithRunStrategy(any(), any());
    SubworkflowInitiator initiator = new SubworkflowInitiator();
    initiator.setAncestors(Collections.singletonList(new UpstreamInitiator.Info()));
    RunRequest request =
        RunRequest.builder()
            .initiator(initiator)
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    WorkflowSummary summary = new WorkflowSummary();
    summary.setWorkflowRunId(10L);
    RunResponse response = actionHandler.start("sample-minimal-wf", "active", request);
    verify(workflowDao, times(1)).getWorkflowDefinition("sample-minimal-wf", "active");
    verify(runStrategyDao, times(1)).startWithRunStrategy(any(), any());
    assertEquals(instance.getWorkflowId(), response.getWorkflowId());
    assertEquals(1L, response.getWorkflowVersionId());
    assertEquals(10L, response.getWorkflowRunId());
    assertEquals("41f0281e-41a2-468d-b830-56141b2f768b", response.getWorkflowUuid());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
  }

  @Test
  public void testStartStopped() {
    when(runStrategyDao.startWithRunStrategy(any(), any())).thenReturn(-1);
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .build();
    RunResponse response = actionHandler.start("sample-minimal-wf", "active", request);
    verify(workflowDao, times(1)).getWorkflowDefinition("sample-minimal-wf", "active");
    verify(runStrategyDao, times(1)).startWithRunStrategy(any(), any());
    assertEquals(instance.getWorkflowId(), response.getWorkflowId());
    assertEquals(1L, response.getWorkflowVersionId());
    assertEquals(0L, response.getWorkflowInstanceId());
    assertEquals(0L, response.getWorkflowRunId());
    assertEquals("41f0281e-41a2-468d-b830-56141b2f768b", response.getWorkflowUuid());
    assertEquals(RunResponse.Status.STOPPED, response.getStatus());
  }

  @Test
  public void testStartDuplicated() {
    when(runStrategyDao.startWithRunStrategy(any(), any())).thenReturn(0);
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    RunResponse response = actionHandler.start("sample-minimal-wf", "active", request);
    verify(workflowDao, times(1)).getWorkflowDefinition("sample-minimal-wf", "active");
    verify(runStrategyDao, times(1)).startWithRunStrategy(any(), any());
    assertEquals(instance.getWorkflowId(), response.getWorkflowId());
    assertEquals(1L, response.getWorkflowVersionId());
    assertEquals(0L, response.getWorkflowInstanceId());
    assertEquals(0L, response.getWorkflowRunId());
    assertEquals("41f0281e-41a2-468d-b830-56141b2f768b", response.getWorkflowUuid());
    assertEquals(RunResponse.Status.DUPLICATED, response.getStatus());
  }

  @Test
  public void testStartWithInvalidTriggers() {
    Stream.of(new SignalInitiator(), new TimeInitiator())
        .forEach(
            initiator -> {
              RunRequest request =
                  RunRequest.builder()
                      .initiator(initiator)
                      .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
                      .build();
              definition.setIsActive(false);
              AssertHelper.assertThrows(
                  "cannot trigger an inactive workflow",
                  IllegalArgumentException.class,
                  "Triggered workflow definition for workflow",
                  () -> actionHandler.start("sample-minimal-wf", "active", request));

              definition.setIsActive(true);
              AssertHelper.assertThrows(
                  "cannot trigger a workflow without trigger uuids",
                  MaestroResourceConflictException.class,
                  "Invalid trigger initiator due to mismatch trigger uuid",
                  () -> actionHandler.start("sample-minimal-wf", "active", request));
            });
  }

  @Test
  public void testStartWithDisabledTriggers() {
    Stream.of(new SignalInitiator(), new TimeInitiator())
        .forEach(
            initiator -> {
              initiator.setTriggerUuid("foo");
              RunRequest request =
                  RunRequest.builder()
                      .initiator(initiator)
                      .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
                      .build();
              PropertiesSnapshot snapshot =
                  definition.getPropertiesSnapshot().toBuilder()
                      .signalTriggerDisabled(true)
                      .timeTriggerDisabled(true)
                      .build();
              definition.setPropertiesSnapshot(snapshot);
              definition.setTriggerUuids(
                  TriggerUuids.builder()
                      .signalTriggerUuids(Collections.singletonMap("foo", 1))
                      .timeTriggerUuid("foo")
                      .build());
              AssertHelper.assertThrows(
                  "cannot trigger a workflow with disabled triggers",
                  MaestroUnprocessableEntityException.class,
                  "is disabled for the workflow [sample-minimal-wf] in the workflow properties.",
                  () -> actionHandler.start("sample-minimal-wf", "active", request));
            });
  }

  @Test
  public void testStartBatch() {
    when(runStrategyDao.startBatchWithRunStrategy(any(), any(), any()))
        .thenReturn(new int[] {1, 0});
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    List<RunResponse> responses =
        actionHandler.startBatch("sample-minimal-wf", "active", Arrays.asList(request, request));
    verify(workflowDao, times(1)).getWorkflowDefinition("sample-minimal-wf", "active");
    verify(runStrategyDao, times(1)).startBatchWithRunStrategy(any(), any(), any());
    assertEquals(2, responses.size());
    assertEquals(1L, responses.get(0).getWorkflowVersionId());
    assertEquals("41f0281e-41a2-468d-b830-56141b2f768b", responses.get(0).getWorkflowUuid());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, responses.get(0).getStatus());
    assertEquals(1L, responses.get(1).getWorkflowVersionId());
    assertEquals("41f0281e-41a2-468d-b830-56141b2f768b", responses.get(1).getWorkflowUuid());
    assertEquals(RunResponse.Status.DUPLICATED, responses.get(1).getStatus());
  }

  @Test
  public void testRunForeachBatch() {
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Optional<Details> errors =
        actionHandler.runForeachBatch(
            definition.getWorkflow(),
            123L,
            1L,
            new RunProperties(),
            "foreach-step",
            new ForeachArtifact(),
            Collections.singletonList(request),
            Collections.singletonList(1L),
            1);
    assertFalse(errors.isPresent());
    verify(instanceDao, times(1)).runWorkflowInstances(any(), any(), eq(1));
  }

  @Test
  public void testRestartRunForeachBatch() {
    ForeachArtifact artifact = new ForeachArtifact();
    artifact.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    artifact.setTotalLoopCount(10);
    artifact.setForeachWorkflowId(instance.getWorkflowId());
    artifact.setForeachRunId(3L);
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Optional<Details> errors =
        actionHandler.runForeachBatch(
            definition.getWorkflow(),
            123L,
            10L,
            new RunProperties(),
            "foreach-step",
            artifact,
            Collections.singletonList(request),
            Collections.singletonList(5L),
            1);
    assertFalse(errors.isPresent());
    ArgumentCaptor<List<WorkflowInstance>> captor = ArgumentCaptor.forClass(List.class);
    verify(instanceDao, times(1))
        .runWorkflowInstances(eq(artifact.getForeachWorkflowId()), captor.capture(), eq(1));
    List<WorkflowInstance> res = captor.getValue();
    assertEquals(1, res.size());
    assertEquals(artifact.getForeachWorkflowId(), res.get(0).getWorkflowId());
    assertEquals(5L, res.get(0).getWorkflowInstanceId());
    assertEquals(3L, res.get(0).getWorkflowRunId());
    assertEquals(10L, res.get(0).getWorkflowVersionId());
  }

  @Test
  public void testCreateRestartForeachInstancesUpstreamModeFromIncomplete() {
    doNothing().when(workflowHelper).updateWorkflowInstance(any(), any());
    when(instanceDao.getLatestWorkflowInstanceRun(anyString(), anyLong()))
        .thenReturn(new WorkflowInstance());

    Map<String, Map<String, ParamDefinition>> stepRunParams =
        Collections.singletonMap(
            "job1",
            Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));

    ForeachArtifact artifact = new ForeachArtifact();
    artifact.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    artifact.setTotalLoopCount(10);
    artifact.setForeachWorkflowId("maestro_foreach_x");
    artifact.setAncestorIterationCount(3L);
    artifact.setForeachRunId(3L);
    artifact.setForeachOverview(new ForeachStepOverview());
    artifact.getForeachOverview().addOne(2, WorkflowInstance.Status.FAILED, null);
    RestartConfig restartConfig =
        RestartConfig.builder()
            .restartPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .stepRestartParams(stepRunParams)
            .addRestartNode("maestro_foreach_x", 1, null)
            .build();

    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("maestro_foreach_x");
    parent.setInstanceId(1);
    parent.setRunId(1);
    parent.setStepId("foreach-step");
    parent.setStepAttemptId(1);
    initiator.setAncestors(Collections.singletonList(parent));

    RunRequest runRequest =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(restartConfig)
            .build();

    Optional<Details> errors =
        actionHandler.runForeachBatch(
            definition.getWorkflow(),
            123L,
            10L,
            new RunProperties(),
            "foreach-step",
            artifact,
            Collections.singletonList(runRequest),
            Collections.singletonList(2L),
            3);
    assertFalse(errors.isPresent());

    verify(instanceDao, times(1)).runWorkflowInstances(any(), any(), anyInt());

    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(workflowHelper, times(1)).updateWorkflowInstance(any(), captor.capture());

    RunRequest res = captor.getValue();
    assertEquals(RunPolicy.RESTART_FROM_INCOMPLETE, res.getCurrentPolicy());
    // it will keep the restart config with step restart params
    assertEquals(restartConfig, res.getRestartConfig());
  }

  @Test
  public void testCreateRestartForeachInstancesUpstreamModeFromBeginning() {
    doNothing().when(workflowHelper).updateWorkflowInstance(any(), any());
    when(instanceDao.getLatestWorkflowInstanceRun(anyString(), anyLong()))
        .thenReturn(new WorkflowInstance());

    Map<String, Map<String, ParamDefinition>> stepRunParams =
        Collections.singletonMap(
            "job1",
            Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));

    ForeachArtifact artifact = new ForeachArtifact();
    artifact.setRunPolicy(RunPolicy.RESTART_FROM_BEGINNING);
    artifact.setTotalLoopCount(10);
    artifact.setForeachWorkflowId("maestro_foreach_x");
    artifact.setAncestorIterationCount(null);
    artifact.setForeachRunId(3L);
    RestartConfig restartConfig =
        RestartConfig.builder()
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .stepRestartParams(stepRunParams)
            .addRestartNode("maestro_foreach_x", 1, null)
            .build();

    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("maestro_foreach_x");
    parent.setInstanceId(1);
    parent.setRunId(1);
    parent.setStepId("foreach-step");
    parent.setStepAttemptId(1);
    initiator.setAncestors(Collections.singletonList(parent));

    RunRequest runRequest =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(restartConfig)
            .build();

    Optional<Details> errors =
        actionHandler.runForeachBatch(
            definition.getWorkflow(),
            123L,
            10L,
            new RunProperties(),
            "foreach-step",
            artifact,
            Collections.singletonList(runRequest),
            Collections.singletonList(2L),
            3);
    assertFalse(errors.isPresent());

    verify(instanceDao, times(1)).runWorkflowInstances(any(), any(), anyInt());

    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(workflowHelper, times(1)).updateWorkflowInstance(any(), captor.capture());

    RunRequest res = captor.getValue();
    assertEquals(RunPolicy.RESTART_FROM_BEGINNING, res.getCurrentPolicy());
    // it will keep the restart config with step restart params
    assertEquals(restartConfig, res.getRestartConfig());
  }

  @Test
  public void testCreateRestartForeachInstancesUpstreamModeFromBeginningWithoutStepParamOverride() {
    doNothing().when(workflowHelper).updateWorkflowInstance(any(), any());
    when(instanceDao.getLatestWorkflowInstanceRun(anyString(), anyLong()))
        .thenReturn(new WorkflowInstance());

    ForeachArtifact artifact = new ForeachArtifact();
    artifact.setRunPolicy(RunPolicy.RESTART_FROM_BEGINNING);
    artifact.setTotalLoopCount(10);
    artifact.setForeachWorkflowId("maestro_foreach_x");
    artifact.setAncestorIterationCount(null);
    artifact.setForeachRunId(3L);
    artifact.setForeachOverview(new ForeachStepOverview());
    artifact.getForeachOverview().addOne(2, WorkflowInstance.Status.FAILED, null);
    RestartConfig restartConfig =
        RestartConfig.builder()
            .restartPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .downstreamPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .addRestartNode("maestro_foreach_x", 1, null)
            .build();

    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("maestro_foreach_x");
    parent.setInstanceId(1);
    parent.setRunId(1);
    parent.setStepId("foreach-step");
    parent.setStepAttemptId(1);
    initiator.setAncestors(Collections.singletonList(parent));

    RunRequest runRequest =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(restartConfig)
            .build();

    Optional<Details> errors =
        actionHandler.runForeachBatch(
            definition.getWorkflow(),
            123L,
            10L,
            new RunProperties(),
            "foreach-step",
            artifact,
            Collections.singletonList(runRequest),
            Collections.singletonList(2L),
            3);
    assertFalse(errors.isPresent());

    verify(instanceDao, times(1)).runWorkflowInstances(any(), any(), anyInt());

    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(workflowHelper, times(1)).updateWorkflowInstance(any(), captor.capture());

    RunRequest res = captor.getValue();
    assertEquals(RunPolicy.RESTART_FROM_BEGINNING, res.getCurrentPolicy());
    assertNull(res.getRestartConfig());
  }

  @Test
  public void testRestartForeachInstance() {
    doNothing().when(workflowHelper).updateWorkflowInstance(any(), any());
    instance.setWorkflowId("maestro_foreach_x");
    RestartConfig restartConfig =
        RestartConfig.builder()
            .restartPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .downstreamPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .addRestartNode("maestro_foreach_x", 1, null)
            .addRestartNode("sample-minimal-wf", 1, "foreach-step")
            .build();

    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info parent = new UpstreamInitiator.Info();
    parent.setWorkflowId("maestro_foreach_x");
    parent.setInstanceId(1);
    parent.setRunId(1);
    parent.setStepId("foreach-step");
    parent.setStepAttemptId(1);
    initiator.setAncestors(Collections.singletonList(parent));

    RunRequest runRequest =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .restartConfig(restartConfig)
            .build();

    assertFalse(
        actionHandler
            .restartForeachInstance(runRequest, instance, "foreach-step", 100)
            .isPresent());
    verify(instanceDao, times(1)).runWorkflowInstances(any(), any(), anyInt());
    assertEquals(100, instance.getWorkflowRunId());

    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(workflowHelper, times(1)).updateWorkflowInstance(any(), captor.capture());

    RunRequest res = captor.getValue();
    assertEquals(RunPolicy.RESTART_FROM_INCOMPLETE, res.getCurrentPolicy());
    assertEquals(
        RestartConfig.builder()
            .restartPolicy(RunPolicy.RESTART_FROM_INCOMPLETE)
            .downstreamPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .addRestartNode("maestro_foreach_x", 1, null)
            .build(),
        res.getRestartConfig());
  }

  @Test
  public void testActivate() {
    when(workflowDao.activate("sample-minimal-wf", "1", tester))
        .thenReturn(
            WorkflowVersionUpdateJobEvent.create("sample-minimal-wf", 1L, 0L, tester, "test log"));
    WorkflowActionResponse response = actionHandler.activate("sample-minimal-wf", "1", tester);
    verify(workflowDao, times(1)).activate("sample-minimal-wf", "1", tester);
    assertEquals("sample-minimal-wf", response.getWorkflowId());
    assertEquals(1, response.getWorkflowVersionId());
    assertEquals("test log", response.getTimelineEvent().getMessage());
  }

  @Test
  public void testActivateError() {
    AssertHelper.assertThrows(
        "caller cannot be null to activate workflow",
        NullPointerException.class,
        "caller cannot be null to activate workflow [sample-minimal-wf][1]",
        () -> actionHandler.activate("sample-minimal-wf", "1", null));
    when(workflowDao.activate("sample-minimal-wf", "0", tester))
        .thenThrow(new InvalidWorkflowVersionException("sample-minimal-wf", "0"));
    AssertHelper.assertThrows(
        "Workflow version must be larger than 0",
        InvalidWorkflowVersionException.class,
        "Invalid workflow version [0] for workflow [sample-minimal-wf]",
        () -> actionHandler.activate("sample-minimal-wf", "0", tester));
  }

  @Test
  public void testDeactivate() {
    when(workflowDao.deactivate("sample-minimal-wf", tester)).thenReturn("foo");
    actionHandler.deactivate("sample-minimal-wf", tester);
    verify(workflowDao, times(1)).deactivate("sample-minimal-wf", tester);
  }

  @Test
  public void testDeactivateError() {
    AssertHelper.assertThrows(
        "caller cannot be null to deactivate workflow",
        NullPointerException.class,
        "caller cannot be null to deactivate workflow [sample-minimal-wf]",
        () -> actionHandler.deactivate("sample-minimal-wf", null));
  }

  @Test
  public void testStop() {
    when(instanceDao.terminateQueuedInstances(
            eq("sample-minimal-wf"),
            eq(Constants.TERMINATE_BATCH_LIMIT),
            eq(WorkflowInstance.Status.STOPPED),
            anyString()))
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT)
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT)
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT - 1);
    when(instanceDao.terminateRunningInstances(
            eq("sample-minimal-wf"),
            eq(Constants.TERMINATE_BATCH_LIMIT),
            eq(Actions.WorkflowInstanceAction.STOP),
            any(),
            anyString()))
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT - 1);
    String res = actionHandler.stop("sample-minimal-wf", tester).getMessage();
    assertEquals("Terminated [29] queued instances and terminating [9] running instances", res);
  }

  @Test
  public void testKill() {
    when(instanceDao.terminateQueuedInstances(
            eq("sample-minimal-wf"),
            eq(Constants.TERMINATE_BATCH_LIMIT),
            eq(WorkflowInstance.Status.FAILED),
            anyString()))
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT)
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT)
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT - 1);
    when(instanceDao.terminateRunningInstances(
            eq("sample-minimal-wf"),
            eq(Constants.TERMINATE_BATCH_LIMIT),
            eq(Actions.WorkflowInstanceAction.KILL),
            any(),
            anyString()))
        .thenReturn(Constants.TERMINATE_BATCH_LIMIT - 1);
    String res = actionHandler.kill("sample-minimal-wf", tester).getMessage();
    assertEquals("Terminated [29] queued instances and terminating [9] running instances", res);
  }

  @Test
  public void testUnblock() {
    when(instanceDao.tryUnblockFailedWorkflowInstances(eq("sample-minimal-wf"), anyInt(), any()))
        .thenReturn(10);
    TimelineEvent event = actionHandler.unblock("sample-minimal-wf", tester);
    assertEquals("Unblocked [10] failed workflow instances.", event.getMessage());
    verify(maestroJobEventPublisher, times(1)).publishOrThrow(any(StartWorkflowJobEvent.class));
  }

  @Test
  public void testUnblockReachingLimit() {
    when(instanceDao.tryUnblockFailedWorkflowInstances(eq("sample-minimal-wf"), anyInt(), any()))
        .thenReturn(Constants.UNBLOCK_BATCH_SIZE)
        .thenReturn(10);
    TimelineEvent event = actionHandler.unblock("sample-minimal-wf", tester);
    assertEquals("Unblocked [110] failed workflow instances.", event.getMessage());
    verify(maestroJobEventPublisher, times(1)).publishOrThrow(any(StartWorkflowJobEvent.class));
  }

  @Test
  public void testUnblockNothing() {
    when(instanceDao.tryUnblockFailedWorkflowInstances(eq("sample-minimal-wf"), anyInt(), any()))
        .thenReturn(0);
    TimelineEvent event = actionHandler.unblock("sample-minimal-wf", tester);
    assertEquals("Unblocked [0] failed workflow instances.", event.getMessage());
    verify(maestroJobEventPublisher, times(0)).publishOrThrow(any(StartWorkflowJobEvent.class));
  }
}
