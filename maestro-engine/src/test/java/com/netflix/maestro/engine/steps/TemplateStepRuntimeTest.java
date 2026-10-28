/*
 * Copyright 2026 Netflix, Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.dto.OutputData;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.params.OutputDataManager;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.engine.templates.PrepareResult;
import com.netflix.maestro.engine.templates.SelTemplatePreparer;
import com.netflix.maestro.engine.templates.StaticTemplatePreparer;
import com.netflix.maestro.engine.templates.TemplateInstanceRunner;
import com.netflix.maestro.engine.templates.TemplatePreparer;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.instance.WorkflowStepStatusSummary;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.parameter.StringParameter;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplateExpansion;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import com.netflix.maestro.models.timeline.TimelineDetailsEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.InstanceActionJobEvent;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class TemplateStepRuntimeTest extends MaestroBaseTest {
  private static final String WORKFLOW_ID = "test-workflow";
  private static final String STEP_ID = "publish";
  private static final String JOB_TYPE = "write_audit_publish";
  private static final String VERSION = "v3";
  private static final String INLINE_ID = "maestro_template_7D3_11_abc";
  private static final String IDENTITY = "[test-workflow][1][1][publish][1]";

  @Mock private TemplateInstanceRunner instanceRunner;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private JobTemplateManager jobTemplateManager;
  @Mock private OutputDataManager outputDataManager;
  @Mock private TemplatePreparer stepPreparer;

  private TemplateStepRuntime templateStepRuntime;
  private WorkflowSummary workflowSummary;
  private TemplateStep step;
  private JobTemplate.Definition definition;

  @Before
  public void setUp() throws Exception {
    when(stepPreparer.type()).thenReturn(TemplatePrepare.STEP);
    when(stepPreparer.adaptStepInputs(any(), any())).thenAnswer(inv -> inv.getArgument(1));
    templateStepRuntime =
        new TemplateStepRuntime(
            instanceRunner,
            instanceDao,
            stepInstanceDao,
            queueSystem,
            jobTemplateManager,
            outputDataManager,
            MAPPER,
            List.of(new StaticTemplatePreparer(), new SelTemplatePreparer(), stepPreparer));

    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId(WORKFLOW_ID);
    workflowSummary.setWorkflowInstanceId(1L);
    workflowSummary.setWorkflowRunId(1L);
    workflowSummary.setInternalId(12345L);
    workflowSummary.setWorkflowVersionId(2L);
    workflowSummary.setInitiator(new ManualInitiator());
    workflowSummary.setRunPolicy(RunPolicy.START_FRESH_NEW_RUN);
    workflowSummary.setRunProperties(new RunProperties());
    workflowSummary.setParams(Map.of("wf_param", buildParam("wf_param", "wf_value")));

    step = loadObject("fixtures/typedsteps/sample-template-step.json", TemplateStep.class);
    definition =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class)
            .getDefinition();
    when(jobTemplateManager.getJobTemplateVersion(workflowSummary, step)).thenReturn(VERSION);
    when(jobTemplateManager.loadDefinition(step, VERSION)).thenReturn(definition);
    when(jobTemplateManager.findDefinition(step, VERSION)).thenReturn(Optional.of(definition));
    when(instanceRunner.run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean()))
        .thenReturn(Optional.empty());
  }

  private StepRuntimeSummary createStepRuntimeSummary(Map<String, Artifact> artifacts) {
    return createStepRuntimeSummary(artifacts, 1L);
  }

  private StepRuntimeSummary createStepRuntimeSummary(
      Map<String, Artifact> artifacts, long stepAttemptId) {
    Map<String, Parameter> params = new HashMap<>();
    params.put("target_table", buildParam("target_table", "db.sample_table"));
    params.put("snapshot_id", buildParam("snapshot_id", "snap-1"));
    params.put("published_snapshot", buildParam("published_snapshot", ""));
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    return StepRuntimeSummary.builder()
        .stepId(STEP_ID)
        .stepAttemptId(stepAttemptId)
        .stepInstanceUuid("step-uuid")
        .type(StepType.TEMPLATE)
        .artifacts(artifacts)
        .params(params)
        .stepRetry(stepRetry)
        .build();
  }

  /** An artifact as {@code start()} leaves it, before any inline instance is launched. */
  private TemplateArtifact createStartedArtifact(String prepareType) {
    TemplateArtifact artifact = new TemplateArtifact();
    artifact.setTemplateWorkflowId(INLINE_ID);
    artifact.setTemplateIdentity(IDENTITY);
    artifact.setPrepareType(prepareType);
    artifact.setTemplateRunId(1L);
    artifact.setJobType(JOB_TYPE);
    artifact.setTemplateVersion(VERSION);
    return artifact;
  }

  /** An artifact whose execution instance has been launched. */
  private TemplateArtifact createArtifact(long runId, Long prepareInstanceId, Long instanceId) {
    TemplateArtifact artifact =
        createStartedArtifact(
            prepareInstanceId == null ? TemplatePrepare.STATIC : TemplatePrepare.STEP);
    artifact.setPrepareInstanceId(prepareInstanceId);
    artifact.setTemplateInstanceId(instanceId);
    artifact.setTemplateRunId(runId);
    artifact.setTemplateUuid("template-uuid");
    return artifact;
  }

  private StepRuntimeSummary createStartedStepRuntimeSummary(String prepareType) {
    return createStepRuntimeSummary(
        Map.of(Artifact.Type.TEMPLATE.key(), createStartedArtifact(prepareType)));
  }

  private static String firstError(StepRuntime.Result result) {
    return ((TimelineDetailsEvent) result.timeline().getFirst()).getErrors().getFirst();
  }

  private static TemplateArtifact templateArtifact(StepRuntime.Result result) {
    return result.artifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
  }

  private static Map<String, ParamDefinition> stepParams(Workflow workflow, String stepId) {
    return workflow.getSteps().stream()
        .filter(s -> s.getId().equals(stepId))
        .findFirst()
        .orElseThrow()
        .getParams();
  }

  private static Map<String, String> successors(Workflow workflow, String stepId) {
    return workflow.getSteps().stream()
        .filter(s -> s.getId().equals(stepId))
        .findFirst()
        .orElseThrow()
        .getTransition()
        .getSuccessors();
  }

  @Test
  public void testStartInitializesArtifact() {
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    assertTrue(result.timeline().isEmpty());
    TemplateArtifact artifact = templateArtifact(result);
    assertTrue(artifact.getTemplateWorkflowId().startsWith("maestro_template_7D3_11_"));
    assertEquals(
        workflowSummary.getIdentity() + runtimeSummary.getIdentity(),
        artifact.getTemplateIdentity());
    assertEquals(TemplatePrepare.STATIC, artifact.getPrepareType());
    assertNull(artifact.getPrepareInstanceId());
    assertNull(artifact.getTemplateInstanceId());
    assertNull(artifact.getTemplateUuid());
    assertEquals(1L, artifact.getTemplateRunId());
    assertEquals(JOB_TYPE, artifact.getJobType());
    assertEquals(VERSION, artifact.getTemplateVersion());
    verify(instanceRunner, never())
        .run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean());
    verify(instanceDao, never()).getLargestTemplateRunIdFromRuns(anyString());
    verify(stepInstanceDao, never()).getLatestTemplateArtifact(anyString(), anyLong(), anyString());
  }

  @Test
  public void testStartRetryTakesNextRunIdAndPreparesAgain() {
    when(instanceDao.getLargestTemplateRunIdFromRuns(anyString())).thenReturn(1L);
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>(), 2L);

    StepRuntime.Result result = templateStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    TemplateArtifact artifact = templateArtifact(result);
    assertEquals(2L, artifact.getTemplateRunId());
    assertNull(artifact.getTemplateInstanceId());
    assertNull(artifact.getExpansion());
    // a retry in a fresh run never reuses the previous attempt's execution instance
    verify(stepInstanceDao, never()).getLatestTemplateArtifact(anyString(), anyLong(), anyString());
  }

  @Test
  public void testStartFatalErrorWhenTemplateMissing() {
    when(jobTemplateManager.loadDefinition(step, VERSION))
        .thenThrow(new MaestroNotFoundException("Cannot find the job template"));
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertTrue(firstError(result).contains("Cannot find the job template"));
  }

  @Test
  public void testStartFatalErrorWhenPreparerUnknown() {
    definition.setPrepare(definition.getPrepare().toBuilder().type("remote").build());
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertTrue(firstError(result).contains("prepare type [remote], which has no preparer"));
  }

  @Test
  public void testStartRethrowsRetryableError() {
    when(jobTemplateManager.loadDefinition(step, VERSION))
        .thenThrow(new MaestroRetryableError("db is down"));
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    AssertHelper.assertThrows(
        "retryable errors are retried by the engine",
        MaestroRetryableError.class,
        "db is down",
        () -> templateStepRuntime.start(workflowSummary, step, runtimeSummary));
  }

  @Test
  public void testStartRestartCopiesPreviousExecutionInstance() {
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    when(instanceDao.getLargestTemplateRunIdFromRuns(anyString())).thenReturn(2L);
    TemplateArtifact prevArtifact = createArtifact(2L, 1L, 2L);
    prevArtifact.setTemplateVersion("v2");
    when(stepInstanceDao.getLatestTemplateArtifact(WORKFLOW_ID, 1L, STEP_ID))
        .thenReturn(prevArtifact);
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.start(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    TemplateArtifact artifact = templateArtifact(result);
    assertEquals(3L, artifact.getTemplateRunId());
    assertEquals(Long.valueOf(2L), artifact.getTemplateInstanceId());
    assertNull(artifact.getPrepareInstanceId());
    assertNull(artifact.getTemplateUuid());
    assertEquals(TemplatePrepare.STEP, artifact.getPrepareType());
    assertEquals("v2", artifact.getTemplateVersion());
    verify(instanceDao).getLargestTemplateRunIdFromRuns(artifact.getTemplateWorkflowId());
  }

  @Test
  public void testStartRestartPreparesAgainWhenPreviousRunNeverLaunchedExecution() {
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    when(instanceDao.getLargestTemplateRunIdFromRuns(anyString())).thenReturn(2L);
    when(stepInstanceDao.getLatestTemplateArtifact(WORKFLOW_ID, 1L, STEP_ID))
        .thenReturn(createArtifact(2L, 1L, null));
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    TemplateArtifact artifact =
        templateArtifact(templateStepRuntime.start(workflowSummary, step, runtimeSummary));

    assertEquals(3L, artifact.getTemplateRunId());
    assertNull(artifact.getTemplateInstanceId());
    assertEquals(TemplatePrepare.STATIC, artifact.getPrepareType());
    assertEquals(VERSION, artifact.getTemplateVersion());
  }

  @Test
  public void testStartRestartWithoutPreviousArtifact() {
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    when(instanceDao.getLargestTemplateRunIdFromRuns(anyString())).thenReturn(0L);
    when(stepInstanceDao.getLatestTemplateArtifact(WORKFLOW_ID, 1L, STEP_ID)).thenReturn(null);
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    TemplateArtifact artifact =
        templateArtifact(templateStepRuntime.start(workflowSummary, step, runtimeSummary));

    assertEquals(1L, artifact.getTemplateRunId());
    assertNull(artifact.getTemplateInstanceId());
  }

  @Test
  public void testStartStepRetryIncrementsRunIdWithoutReusingInstance() {
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());
    runtimeSummary.getStepRetry().setRetryable(false);
    when(instanceDao.getLargestTemplateRunIdFromRuns(anyString())).thenReturn(1L);

    TemplateArtifact artifact =
        templateArtifact(templateStepRuntime.start(workflowSummary, step, runtimeSummary));

    assertEquals(2L, artifact.getTemplateRunId());
    assertNull(artifact.getTemplateInstanceId());
    // a fresh run never reuses the previous execution instance
    verify(stepInstanceDao, never()).getLatestTemplateArtifact(anyString(), anyLong(), anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteStaticTemplateSplicesInputsAndLaunchesExecutionInstance() {
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.STATIC);

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    TemplateArtifact artifact = templateArtifact(result);
    assertNull(artifact.getPrepareInstanceId());
    assertEquals(Long.valueOf(1L), artifact.getTemplateInstanceId());
    assertEquals(1L, artifact.getTemplateRunId());
    assertEquals(
        "Started a template execution instance: " + artifact.getIdentity(),
        ((TimelineLogEvent) result.timeline().getFirst()).getMessage());

    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    ArgumentCaptor<Map<String, ParamDefinition>> paramsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(instanceRunner)
        .run(
            eq(workflowSummary),
            eq(runtimeSummary),
            eq(step),
            eq(artifact),
            eq(1L),
            workflowCaptor.capture(),
            paramsCaptor.capture(),
            eq(false));
    Workflow inlineWorkflow = workflowCaptor.getValue();
    assertEquals(INLINE_ID, inlineWorkflow.getId());
    assertEquals(IDENTITY, inlineWorkflow.getName());
    assertEquals(
        List.of(
            "write",
            "audit",
            "publish_snapshot",
            "write_a",
            "write_b",
            "writes",
            "announce",
            Constants.TEMPLATE_OUTPUTS_STEP_ID),
        inlineWorkflow.getAllStepIds());
    // the writes position: write_a -> write_b -> join step "writes" -> audit
    assertEquals(Map.of("write_b", "true"), successors(inlineWorkflow, "write_a"));
    assertEquals(Map.of("writes", "true"), successors(inlineWorkflow, "write_b"));
    assertEquals(Map.of("audit", "true"), successors(inlineWorkflow, "writes"));
    assertEquals(Map.of("audit", "true"), successors(inlineWorkflow, "write"));
    ParamDefinition collected = stepParams(inlineWorkflow, "writes").get("row_count");
    assertEquals(ParamType.LONG_ARRAY, collected.getType());
    assertEquals(
        "return new long[]{write_a__row_count, write_b__row_count};", collected.getExpression());
    // the after_publish position: publish_snapshot -> announce -> collector, with the promised
    // param added to the attached step
    assertEquals(Map.of("announce", "true"), successors(inlineWorkflow, "publish_snapshot"));
    assertEquals(
        Map.of(Constants.TEMPLATE_OUTPUTS_STEP_ID, "true"), successors(inlineWorkflow, "announce"));
    ParamDefinition exposed = stepParams(inlineWorkflow, "announce").get("published_snapshot");
    assertEquals("publish_snapshot__snapshot;", exposed.getExpression());
    assertEquals(ParamMode.IMMUTABLE, exposed.getMode());
    assertEquals(ParamSource.TEMPLATE_OUTPUT, exposed.getSource());
    // the collector carries the promised params with the template output source
    Map<String, ParamDefinition> collector = inlineWorkflow.getSteps().getLast().getParams();
    assertEquals(Set.of("published_snapshot"), collector.keySet());
    assertEquals(
        "publish_snapshot__snapshot;", collector.get("published_snapshot").getExpression());
    assertEquals(ParamSource.TEMPLATE_OUTPUT, collector.get("published_snapshot").getSource());
    // the artifact keeps the expansion as the preparer returned it, without the step inputs
    assertEquals(
        List.of("write", "audit", "publish_snapshot"),
        artifact.getExpansion().getSteps().stream().map(Step::getId).toList());
    // the supplied steps in the step definition are not mutated by the splice
    assertTrue(
        step.getStepInputs().get("writes").getLast().getTransition().getSuccessors().isEmpty());
    assertFalse(
        step.getStepInputs()
            .get("after_publish")
            .getFirst()
            .getParams()
            .containsKey("published_snapshot"));
    assertEquals(
        Map.of("audit", "true"),
        definition.getPrepare().getSteps().getFirst().getTransition().getSuccessors());

    Map<String, ParamDefinition> runParams = paramsCaptor.getValue();
    assertEquals(
        List.of("wf_param", "target_table", "snapshot_id"), List.copyOf(runParams.keySet()));
    assertEquals("db.sample_table", runParams.get("target_table").asStringParamDef().getValue());
  }

  @Test
  public void testExecuteRetryLaterWhenLaunchFails() {
    when(instanceRunner.run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean()))
        .thenReturn(Optional.of(Details.create("failed to insert")));
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.STATIC);

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
  }

  @Test
  public void testExecuteFatalErrorWhenTemplateMissing() {
    when(jobTemplateManager.loadDefinition(step, VERSION))
        .thenThrow(new MaestroNotFoundException("Cannot find the job template"));
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.STATIC);

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
  }

  @Test
  public void testExecuteFatalErrorWhenSuppliedStepCollides() {
    TypedStep colliding = new TypedStep();
    colliding.setId("audit");
    colliding.setType(StepType.NOOP);
    step.setStepInputs(Map.of("writes", List.of(colliding)));
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.STATIC);

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(firstError(result).contains("collides"));
    verify(instanceRunner, never())
        .run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean());
  }

  @Test
  public void testExecuteStepPreparerPendingWithArtifact() {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    when(stepPreparer.prepare(any()))
        .thenAnswer(
            invocation -> {
              com.netflix.maestro.engine.templates.PrepareContext context =
                  invocation.getArgument(0);
              context.artifact().setPrepareInstanceId(1L);
              return PrepareResult.pending(TimelineLogEvent.info("started prepare"), true);
            });
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.STEP);

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    TemplateArtifact artifact = templateArtifact(result);
    assertEquals(TemplatePrepare.STEP, artifact.getPrepareType());
    assertEquals(Long.valueOf(1L), artifact.getPrepareInstanceId());
    assertNull(artifact.getTemplateInstanceId());
    assertEquals("started prepare", ((TimelineLogEvent) result.timeline().getFirst()).getMessage());
    verify(instanceRunner, never())
        .run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean());
  }

  @Test
  public void testPreparePendingWithoutArtifactChange() {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    when(stepPreparer.prepare(any())).thenReturn(PrepareResult.pending(null, false));
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(
            Map.of(Artifact.Type.TEMPLATE.key(), createArtifact(1L, 1L, null)));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertTrue(result.timeline().isEmpty());
  }

  @Test
  public void testPrepareFailed() {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    when(stepPreparer.prepare(any()))
        .thenReturn(
            PrepareResult.failed(StepRuntime.State.FATAL_ERROR, TimelineLogEvent.info("failed")));
    TemplateArtifact artifact = createArtifact(1L, 1L, null);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertEquals(artifact, templateArtifact(result));
    assertEquals("failed", ((TimelineLogEvent) result.timeline().getFirst()).getMessage());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrepareReadyLaunchesExecutionInstanceAfterPrepareInstance() throws Exception {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    // the expansion has no publish_snapshot step, so only the writes position is used
    step.getStepInputs().remove("after_publish");
    TemplateExpansion expansion =
        loadObject("fixtures/stepruntime/sample-template-expansion.json", TemplateExpansion.class);
    when(stepPreparer.prepare(any()))
        .thenReturn(PrepareResult.ready(expansion, TimelineLogEvent.info("prepared")));
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(
            Map.of(Artifact.Type.TEMPLATE.key(), createArtifact(1L, 1L, null)));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    TemplateArtifact artifact = templateArtifact(result);
    assertEquals(Long.valueOf(1L), artifact.getPrepareInstanceId());
    assertEquals(Long.valueOf(2L), artifact.getTemplateInstanceId());
    assertEquals(
        List.of("prep", "write_jobs", "audit"),
        artifact.getExpansion().getSteps().stream().map(Step::getId).toList());
    assertEquals("wap-3f2a", artifact.getExpansion().getParams().get("wap_branch").getValue());
    // the preparer's event comes before the launch event
    assertEquals("prepared", ((TimelineLogEvent) result.timeline().getFirst()).getMessage());

    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    ArgumentCaptor<Map<String, ParamDefinition>> paramsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(instanceRunner)
        .run(
            any(),
            any(),
            eq(step),
            eq(artifact),
            eq(2L),
            workflowCaptor.capture(),
            paramsCaptor.capture(),
            eq(false));
    // the writes position lands in front of audit, through its join step
    assertEquals(
        List.of(
            "prep",
            "write_jobs",
            "audit",
            "write_a",
            "write_b",
            "writes",
            Constants.TEMPLATE_OUTPUTS_STEP_ID),
        workflowCaptor.getValue().getAllStepIds());
    assertEquals("wap-3f2a", paramsCaptor.getValue().get("wap_branch").getValue());
    assertEquals("db.sample_table", paramsCaptor.getValue().get("target_table").getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPreparerAdaptsStepInputsBeforeSplice() throws Exception {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    step.getStepInputs().remove("after_publish");
    TemplateExpansion expansion =
        loadObject("fixtures/stepruntime/sample-template-expansion.json", TemplateExpansion.class);
    when(stepPreparer.prepare(any()))
        .thenReturn(PrepareResult.ready(expansion, TimelineLogEvent.info("prepared")));
    // the preparer tags every attached write step
    when(stepPreparer.adaptStepInputs(any(), any()))
        .thenAnswer(
            inv -> {
              Map<String, List<Step>> inputs = inv.getArgument(1);
              inputs
                  .get("writes")
                  .forEach(
                      s ->
                          s.getParams()
                              .put(
                                  "write_tag",
                                  StringParamDefinition.builder()
                                      .name("write_tag")
                                      .value("wap-1")
                                      .build()));
              return inputs;
            });
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(
            Map.of(Artifact.Type.TEMPLATE.key(), createArtifact(1L, 1L, null)));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    verify(instanceRunner)
        .run(any(), any(), eq(step), any(), eq(2L), workflowCaptor.capture(), any(), eq(false));
    Workflow inlineWorkflow = workflowCaptor.getValue();
    // the adapted copies are what gets spliced
    assertEquals("wap-1", stepParams(inlineWorkflow, "write_a").get("write_tag").getValue());
    assertEquals("wap-1", stepParams(inlineWorkflow, "write_b").get("write_tag").getValue());
    // the step definition's own inputs are untouched
    assertEquals(
        List.of("write_a", "write_b"),
        step.getStepInputs().get("writes").stream().map(Step::getId).toList());
    assertFalse(step.getStepInputs().get("writes").getFirst().getParams().containsKey("write_tag"));
  }

  @Test
  public void testPreparerReturningNullStepInputsIsFatal() {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    when(stepPreparer.prepare(any()))
        .thenReturn(
            PrepareResult.ready(
                TemplateExpansion.builder()
                    .steps(List.of(definition.getPrepare().getSteps().getFirst()))
                    .build(),
                null));
    when(stepPreparer.adaptStepInputs(any(), any())).thenReturn(null);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(
            Map.of(Artifact.Type.TEMPLATE.key(), createArtifact(1L, 1L, null)));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(firstError(result).contains("preparer [step] returned null step inputs"));
  }

  @Test
  public void testPrepareReadyWithoutStepsIsFatal() {
    definition.setPrepare(definition.getPrepare().toBuilder().type(TemplatePrepare.STEP).build());
    when(stepPreparer.prepare(any()))
        .thenReturn(PrepareResult.ready(TemplateExpansion.builder().build(), null));
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(
            Map.of(Artifact.Type.TEMPLATE.key(), createArtifact(1L, 1L, null)));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(firstError(result).contains("without steps"));
  }

  @Test
  public void testExecuteRestartsPreviousExecutionInstance() {
    // as start() leaves a restart: the previous execution instance id without a uuid for this run
    TemplateArtifact artifact = createArtifact(3L, null, 2L);
    artifact.setTemplateUuid(null);
    artifact.setPrepareType(TemplatePrepare.STEP);
    artifact.setTemplateVersion("v2");
    JobTemplate.Definition prevDefinition = new JobTemplate.Definition();
    prevDefinition.setParams(Map.of());
    when(jobTemplateManager.loadDefinition(step, "v2")).thenReturn(prevDefinition);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertEquals(artifact, templateArtifact(result));
    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    verify(instanceRunner)
        .run(
            eq(workflowSummary),
            eq(runtimeSummary),
            eq(step),
            eq(artifact),
            eq(2L),
            workflowCaptor.capture(),
            any(),
            eq(true));
    assertTrue(workflowCaptor.getValue().getSteps().isEmpty());
    assertEquals(IDENTITY, workflowCaptor.getValue().getName());
    verify(stepPreparer, never()).prepare(any());
    verify(instanceDao, never()).getWorkflowInstanceRun(anyString(), anyLong(), anyLong());
  }

  private WorkflowInstance mockExecutionInstance(
      TemplateArtifact artifact, WorkflowInstance.Status status, long completedWithError) {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId(INLINE_ID);
    instance.setWorkflowInstanceId(artifact.getTemplateInstanceId());
    instance.setWorkflowRunId(artifact.getTemplateRunId());
    instance.setStatus(status);
    WorkflowRuntimeOverview overview = new WorkflowRuntimeOverview();
    overview.setTotalStepCount(3);
    EnumMap<StepInstance.Status, WorkflowStepStatusSummary> stepOverview =
        new EnumMap<>(StepInstance.Status.class);
    stepOverview.put(StepInstance.Status.SUCCEEDED, WorkflowStepStatusSummary.of(2));
    if (completedWithError > 0) {
      stepOverview.put(
          StepInstance.Status.COMPLETED_WITH_ERROR,
          WorkflowStepStatusSummary.of(completedWithError));
    }
    overview.setStepOverview(stepOverview);
    instance.setRuntimeOverview(overview);
    when(instanceDao.getWorkflowInstanceRun(
            INLINE_ID, artifact.getTemplateInstanceId(), artifact.getTemplateRunId()))
        .thenReturn(instance);
    return instance;
  }

  private void mockCollectorView(TemplateArtifact artifact, Map<String, Parameter> params) {
    StepInstance collector = new StepInstance();
    collector.setParams(params);
    when(stepInstanceDao.getStepInstanceView(
            INLINE_ID, artifact.getTemplateInstanceId(), Constants.TEMPLATE_OUTPUTS_STEP_ID))
        .thenReturn(collector);
  }

  @Test
  public void testTrackExecutionInstance() {
    Map<WorkflowInstance.Status, StepRuntime.State> expected = new HashMap<>();
    expected.put(WorkflowInstance.Status.CREATED, StepRuntime.State.CONTINUE);
    expected.put(WorkflowInstance.Status.IN_PROGRESS, StepRuntime.State.CONTINUE);
    expected.put(WorkflowInstance.Status.PAUSED, StepRuntime.State.CONTINUE);
    expected.put(WorkflowInstance.Status.FAILED, StepRuntime.State.USER_ERROR);
    expected.put(WorkflowInstance.Status.STOPPED, StepRuntime.State.STOPPED);
    expected.put(WorkflowInstance.Status.TIMED_OUT, StepRuntime.State.TIMED_OUT);

    expected.forEach(
        (status, state) -> {
          TemplateArtifact artifact = createArtifact(1L, null, 1L);
          WorkflowInstance instance = mockExecutionInstance(artifact, status, 0);
          StepRuntimeSummary runtimeSummary =
              createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

          StepRuntime.Result result =
              templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

          assertEquals(state, result.state());
          assertEquals(
              instance.getRuntimeOverview(), templateArtifact(result).getTemplateOverview());
          assertEquals(status.isTerminal() ? 1 : 0, result.timeline().size());
        });
    verify(outputDataManager, never()).saveOutputData(any());
  }

  @Test
  public void testTrackSucceededExecutionInstancePublishesOutputParams() {
    TemplateArtifact artifact = createArtifact(1L, null, 1L);
    mockExecutionInstance(artifact, WorkflowInstance.Status.SUCCEEDED, 0);
    // the collector evaluates the output param from an expression, as a registered template does
    Parameter published =
        StringParameter.builder()
            .name("published_snapshot")
            .expression("publish_snapshot__snapshot;")
            .evaluatedResult("snap-1")
            .evaluatedTime(123L)
            .build();
    mockCollectorView(
        artifact,
        Map.of("published_snapshot", published, "workflow_id", buildParam("workflow_id", "x")));
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    ArgumentCaptor<OutputData> captor = ArgumentCaptor.forClass(OutputData.class);
    verify(outputDataManager).saveOutputData(captor.capture());
    OutputData outputData = captor.getValue();
    assertEquals(StepType.TEMPLATE, outputData.getExternalJobType());
    assertEquals("step-uuid", outputData.getExternalJobId());
    assertEquals(WORKFLOW_ID, outputData.getWorkflowId());
    assertEquals(Set.of("published_snapshot"), outputData.getParams().keySet());
    // published as a literal value, never as the collector's expression
    StringParameter saved = outputData.getParams().get("published_snapshot").asStringParam();
    assertEquals("snap-1", saved.getValue());
    assertNull(saved.getExpression());
    // the output-data merge onto the parent step accepts MUTABLE params only
    assertEquals(ParamMode.MUTABLE, saved.getMode());
    assertEquals("snap-1", saved.getEvaluatedResult());
    assertEquals(Long.valueOf(123L), saved.getEvaluatedTime());
    assertTrue(outputData.getArtifacts().isEmpty());
  }

  @Test
  public void testTrackSucceededExecutionInstanceCopiesCompletedWithError() {
    TemplateArtifact artifact = createArtifact(1L, null, 1L);
    mockExecutionInstance(artifact, WorkflowInstance.Status.SUCCEEDED, 1);
    mockCollectorView(
        artifact, Map.of("published_snapshot", buildParam("published_snapshot", "snap-1")));
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.COMPLETED_WITH_ERROR, result.state());
    assertTrue(
        result.timeline().getFirst().getMessage().contains("COMPLETED_WITH_ERROR status because"));
    verify(outputDataManager).saveOutputData(any());
  }

  @Test
  public void testTrackSucceededExecutionInstanceWithoutPromisedParams() {
    definition.setParams(Map.of("target_table", definition.getParams().get("target_table")));
    TemplateArtifact artifact = createArtifact(1L, null, 1L);
    mockExecutionInstance(artifact, WorkflowInstance.Status.SUCCEEDED, 0);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.DONE, result.state());
    verify(outputDataManager, never()).saveOutputData(any());
    verify(stepInstanceDao, never()).getStepInstanceView(anyString(), anyLong(), anyString());
  }

  @Test
  public void testTrackSucceededExecutionInstanceMissingOutputParamIsFatal() {
    TemplateArtifact artifact = createArtifact(1L, null, 1L);
    mockExecutionInstance(artifact, WorkflowInstance.Status.SUCCEEDED, 0);
    mockCollectorView(artifact, Map.of());
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(
        firstError(result).contains("did not evaluate the output param [published_snapshot]"));
    verify(outputDataManager, never()).saveOutputData(any());
  }

  @Test
  public void testTerminateWithoutArtifact() {
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertTrue(result.artifacts().isEmpty());
    verify(instanceDao, never()).getWorkflowInstanceStatus(anyString(), anyLong(), anyLong());
  }

  @Test
  public void testTerminateWithoutLaunchedInstance() {
    TemplateArtifact artifact = createArtifact(1L, null, null);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    verify(instanceDao, never()).getWorkflowInstanceStatus(anyString(), anyLong(), anyLong());
  }

  @Test
  public void testTerminateWithWakeUpUnderlyingActor() {
    workflowSummary.setGroupInfo(5L);
    TemplateArtifact artifact = createArtifact(3L, 1L, 2L);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus(INLINE_ID, 2L, 3L))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);

    AssertHelper.assertThrows(
        "should throw retryable error since status is not terminal",
        MaestroRetryableError.class,
        "is not done and will retry it",
        () -> templateStepRuntime.terminate(workflowSummary, runtimeSummary));

    verify(queueSystem)
        .notify(
            argThat(
                msg -> {
                  if (!msg.msgId().equals("[FLOW][" + INLINE_ID + "]1")) {
                    return false;
                  }
                  if (msg.event() instanceof InstanceActionJobEvent event) {
                    return event.getWorkflowId().equals(INLINE_ID)
                        && event.getGroupInfo() == 5L
                        && event.getInstanceRunIds().size() == 1
                        && event.getInstanceRunIds().get(2L) == 3L;
                  }
                  return false;
                }));
  }

  @Test
  public void testTerminateQueuedPrepareInstance() {
    TemplateArtifact artifact = createArtifact(1L, 1L, null);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus(INLINE_ID, 1L, 1L))
        .thenReturn(WorkflowInstance.Status.CREATED);
    WorkflowInstance instance = new WorkflowInstance();
    instance.setStatus(WorkflowInstance.Status.CREATED);
    when(instanceDao.getWorkflowInstanceRun(INLINE_ID, 1L, 1L)).thenReturn(instance);

    AssertHelper.assertThrows(
        "should throw retryable error since status is not terminal",
        MaestroRetryableError.class,
        "is not done and will retry it",
        () -> templateStepRuntime.terminate(workflowSummary, runtimeSummary));

    verify(instanceDao)
        .tryTerminateQueuedInstance(eq(instance), eq(WorkflowInstance.Status.STOPPED), anyString());
  }

  @Test
  public void testTerminateTerminatedInstance() {
    TemplateArtifact artifact = createArtifact(1L, null, 1L);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus(INLINE_ID, 1L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    WorkflowInstance instance = new WorkflowInstance();
    instance.setStatus(WorkflowInstance.Status.STOPPED);
    instance.setRuntimeOverview(new WorkflowRuntimeOverview());
    when(instanceDao.getWorkflowInstanceRun(INLINE_ID, 1L, 1L)).thenReturn(instance);

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertEquals(instance.getRuntimeOverview(), templateArtifact(result).getTemplateOverview());
    assertEquals(
        "Terminated the running template inline workflow instance: [" + INLINE_ID + "][1][1]",
        ((TimelineLogEvent) result.timeline().getFirst()).getMessage());
  }

  /** A stop during the prepare phase terminates the prepare instance and names it. */
  @Test
  public void testTerminateStoppedPrepareInstance() {
    TemplateArtifact artifact = createArtifact(1L, 1L, null);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus(INLINE_ID, 1L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    WorkflowInstance instance = new WorkflowInstance();
    instance.setStatus(WorkflowInstance.Status.STOPPED);
    when(instanceDao.getWorkflowInstanceRun(INLINE_ID, 1L, 1L)).thenReturn(instance);

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertEquals(
        "Terminated the running template inline workflow instance: [" + INLINE_ID + "][1][1]",
        ((TimelineLogEvent) result.timeline().getFirst()).getMessage());
  }

  @Test
  public void testTerminateNotFoundInstance() {
    TemplateArtifact artifact = createArtifact(1L, null, 1L);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus(INLINE_ID, 1L, 1L)).thenReturn(null);

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
  }

  @Test
  public void testInjectRuntimeParamsDeclaresOutputParams() {
    Map<String, ParamDefinition> registered =
        Map.of("target_table", buildParam("target_table", "db.sample_table").toDefinition());
    when(jobTemplateManager.loadRuntimeParams(workflowSummary, step)).thenReturn(registered);

    Map<String, ParamDefinition> params =
        templateStepRuntime.injectRuntimeParams(workflowSummary, step);

    assertEquals(2, params.size());
    assertEquals(registered.get("target_table"), params.get("target_table"));
    ParamDefinition published = params.get("published_snapshot");
    assertEquals(ParamType.STRING, published.getType());
    assertEquals("", published.getValue());
    assertNull(published.getExpression());
    assertEquals(ParamMode.MUTABLE, published.getMode());
    assertFalse(params.containsKey(Constants.TEMPLATE_EXPANSION_PARAM));
  }

  @Test
  public void testInjectRuntimeParamsDeclaresTypedOutputDefaults() {
    Map<String, ParamDefinition> promised = new HashMap<>();
    for (ParamType type : ParamType.values()) {
      promised.put(
          type.name().toLowerCase(),
          MAPPER.convertValue(
              Map.of(
                  "type",
                  type.name(),
                  "expression",
                  "audit__" + type.name() + ";",
                  "mode",
                  ParamMode.IMMUTABLE.name(),
                  "source",
                  ParamSource.TEMPLATE_OUTPUT.name()),
              ParamDefinition.class));
    }
    definition.setParams(promised);
    when(jobTemplateManager.loadRuntimeParams(workflowSummary, step)).thenReturn(Map.of());

    Map<String, ParamDefinition> params =
        templateStepRuntime.injectRuntimeParams(workflowSummary, step);

    assertEquals(ParamType.values().length, params.size());
    params.forEach(
        (name, def) -> {
          assertEquals(ParamType.valueOf(name.toUpperCase()), def.getType());
          assertTrue(def.isLiteral());
          assertEquals(ParamMode.MUTABLE, def.getMode());
          def.validate();
        });
    assertEquals(0L, (long) params.get("long").asLongParamDef().getValue());
    assertEquals(Boolean.FALSE, params.get("boolean").asBooleanParamDef().getValue());
  }

  @Test
  public void testExecuteSelPreparerIsNotImplemented() {
    definition.setPrepare(
        TemplatePrepare.builder()
            .type(TemplatePrepare.SEL)
            .expression("return buildExpansion(target_table);")
            .build());
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.SEL);

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(firstError(result).contains("sel preparer, which is not implemented yet"));
    verify(instanceRunner, never())
        .run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean());
  }

  @Test
  public void testInjectRuntimeParamsWithoutTemplate() {
    when(jobTemplateManager.loadRuntimeParams(workflowSummary, step)).thenReturn(Map.of());
    when(jobTemplateManager.findDefinition(step, VERSION)).thenReturn(Optional.empty());
    assertTrue(templateStepRuntime.injectRuntimeParams(workflowSummary, step).isEmpty());
  }

  @Test
  public void testInjectRuntimeTags() {
    when(jobTemplateManager.loadTags(workflowSummary, step)).thenReturn(List.of(Tag.create("foo")));
    assertEquals(
        List.of(Tag.create("foo")), templateStepRuntime.injectRuntimeTags(workflowSummary, step));
  }

  @Test
  public void testExecuteWithUnexpectedStepType() throws Exception {
    Step typedStep = loadObject("fixtures/typedsteps/sample-typed-step.json", Step.class);
    when(jobTemplateManager.loadDefinition(typedStep, VERSION)).thenReturn(definition);
    StepRuntimeSummary runtimeSummary = createStartedStepRuntimeSummary(TemplatePrepare.STATIC);

    // a non-template step has no step inputs, so the prepared steps run as registered
    StepRuntime.Result result =
        templateStepRuntime.execute(workflowSummary, typedStep, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    verify(instanceRunner)
        .run(
            any(), any(), eq(typedStep), any(), eq(1L), workflowCaptor.capture(), any(), eq(false));
    assertEquals(
        List.of("write", "audit", "publish_snapshot", Constants.TEMPLATE_OUTPUTS_STEP_ID),
        workflowCaptor.getValue().getAllStepIds());
  }
}
