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
package com.netflix.maestro.engine.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.steps.StepRuntime;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class StepTemplatePreparerTest extends MaestroBaseTest {
  private static final String INLINE_ID = "maestro_template_7D3_11_abc";

  @Mock private TemplateInstanceRunner instanceRunner;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;

  private StepTemplatePreparer preparer;
  private JobTemplate.Definition definition;
  private TemplateStep step;
  private TemplateArtifact artifact;
  private PrepareContext context;

  @Before
  public void setUp() throws Exception {
    preparer = new StepTemplatePreparer(instanceRunner, instanceDao, stepInstanceDao, MAPPER);
    definition =
        loadObject("fixtures/stepruntime/job_template_with_step_preparer.json", JobTemplate.class)
            .getDefinition();
    step = loadObject("fixtures/typedsteps/sample-template-step.json", TemplateStep.class);
    step.setSubType("dynamic_publish");
    WorkflowSummary workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("test-workflow");
    workflowSummary.setWorkflowInstanceId(1L);
    workflowSummary.setWorkflowRunId(1L);
    workflowSummary.setParams(Map.of("wf_param", buildParam("wf_param", "wf_value")));
    Map<String, Parameter> params = new HashMap<>();
    params.put("target_table", buildParam("target_table", "core.trips"));
    params.put("row_count", buildParam("row_count", ""));
    StepRuntimeSummary runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("publish")
            .stepAttemptId(1L)
            .stepInstanceUuid("uuid")
            .type(StepType.TEMPLATE)
            .params(params)
            .build();
    artifact = new TemplateArtifact();
    artifact.setTemplateWorkflowId(INLINE_ID);
    artifact.setTemplateIdentity("[test-workflow][1][1][publish][1]");
    artifact.setTemplateRunId(1L);
    artifact.setTemplateVersion("v1");
    artifact.setPrepareType(TemplatePrepare.STEP);
    context = new PrepareContext(workflowSummary, step, runtimeSummary, definition, artifact);
    when(instanceRunner.run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean()))
        .thenReturn(Optional.empty());
  }

  @Test
  public void testType() {
    assertEquals(TemplatePrepare.STEP, preparer.type());
    assertTrue(preparer.injectParams(definition).isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPrepareLaunchesGenerator() throws Exception {
    PrepareResult result = preparer.prepare(context);

    assertFalse(result.isReady());
    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifactChanged());
    assertEquals(
        "Started the template prepare instance: [" + INLINE_ID + "][1][1]",
        ((TimelineLogEvent) result.event()).getMessage());
    assertEquals(Long.valueOf(1L), artifact.getPrepareInstanceId());
    assertNull(artifact.getTemplateInstanceId());

    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    ArgumentCaptor<Map<String, ParamDefinition>> paramsCaptor = ArgumentCaptor.forClass(Map.class);
    verify(instanceRunner)
        .run(
            eq(context.workflowSummary()),
            eq(context.runtimeSummary()),
            eq(step),
            eq(artifact),
            eq(1L),
            workflowCaptor.capture(),
            paramsCaptor.capture(),
            eq(false));
    Workflow inlineWorkflow = workflowCaptor.getValue();
    assertEquals(INLINE_ID, inlineWorkflow.getId());
    assertEquals("[test-workflow][1][1][publish][1]", inlineWorkflow.getName());
    assertEquals(1, inlineWorkflow.getSteps().size());
    Step generator = inlineWorkflow.getSteps().getFirst();
    assertEquals("generate", generator.getId());
    ParamDefinition expansion = generator.getParams().get(Constants.TEMPLATE_EXPANSION_PARAM);
    assertEquals("", expansion.getValue());
    assertEquals(ParamMode.MUTABLE, expansion.getMode());
    // the registered definition is not mutated
    assertNull(definition.getPrepare().getStep().getParams());

    Map<String, ParamDefinition> runParams = paramsCaptor.getValue();
    assertEquals(
        List.of(
            "wf_param",
            "target_table",
            Constants.TEMPLATE_STEP_INPUTS_PARAM,
            Constants.TEMPLATE_IDENTITY_PARAM),
        List.copyOf(runParams.keySet()));
    assertEquals(
        "{\"job_type\":\"dynamic_publish\",\"version\":\"v1\"}",
        runParams.get(Constants.TEMPLATE_IDENTITY_PARAM).getValue());
    Map<String, List<Step>> stepInputs =
        MAPPER.readValue(
            runParams.get(Constants.TEMPLATE_STEP_INPUTS_PARAM).asStringParamDef().getValue(),
            new com.fasterxml.jackson.core.type.TypeReference<>() {});
    assertEquals(List.of("writes", "after_publish"), List.copyOf(stepInputs.keySet()));
    assertEquals(
        List.of("write_a", "write_b"), stepInputs.get("writes").stream().map(Step::getId).toList());
    assertEquals(
        Map.of("write_b", "true"),
        stepInputs.get("writes").getFirst().getTransition().getSuccessors());
  }

  @Test
  public void testPrepareLaunchFailureRetriesLater() {
    when(instanceRunner.run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean()))
        .thenReturn(Optional.of(Details.create("failed to insert")));

    PrepareResult result = preparer.prepare(context);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertFalse(result.artifactChanged());
    assertNull(artifact.getPrepareInstanceId());
  }

  @Test
  public void testPrepareWaitsForRunningPrepareInstance() {
    artifact.setPrepareInstanceId(1L);
    for (WorkflowInstance.Status status :
        List.of(
            WorkflowInstance.Status.CREATED,
            WorkflowInstance.Status.IN_PROGRESS,
            WorkflowInstance.Status.PAUSED)) {
      mockPrepareInstance(status);
      PrepareResult result = preparer.prepare(context);
      assertEquals(StepRuntime.State.CONTINUE, result.state());
      assertFalse(result.artifactChanged());
      assertNull(result.event());
    }
    verify(instanceRunner, never())
        .run(any(), any(), any(), any(), anyLong(), any(), any(), anyBoolean());
  }

  @Test
  public void testPrepareFailsWithPrepareInstance() {
    artifact.setPrepareInstanceId(1L);
    Map<WorkflowInstance.Status, StepRuntime.State> expected =
        Map.of(
            WorkflowInstance.Status.FAILED, StepRuntime.State.FATAL_ERROR,
            WorkflowInstance.Status.STOPPED, StepRuntime.State.STOPPED,
            WorkflowInstance.Status.TIMED_OUT, StepRuntime.State.TIMED_OUT);
    expected.forEach(
        (status, state) -> {
          mockPrepareInstance(status);
          PrepareResult result = preparer.prepare(context);
          assertEquals(state, result.state());
          assertTrue(((TimelineLogEvent) result.event()).getMessage().contains(status.name()));
        });
  }

  @Test
  public void testPrepareReadsExpansionFromGenerator() throws Exception {
    artifact.setPrepareInstanceId(1L);
    WorkflowInstance instance = mockPrepareInstance(WorkflowInstance.Status.SUCCEEDED);
    String payload =
        MAPPER.writeValueAsString(
            loadObject("fixtures/stepruntime/sample-template-expansion.json", Object.class));
    mockGeneratorView(buildParam(Constants.TEMPLATE_EXPANSION_PARAM, payload));

    PrepareResult result = preparer.prepare(context);

    assertTrue(result.isReady());
    assertEquals(
        List.of("prep", "write_jobs", "audit"),
        result.expansion().getSteps().stream().map(Step::getId).toList());
    assertEquals(
        "Template prepare instance "
            + instance.getIdentity()
            + " SUCCEEDED, expansion has 3 steps and 1 params",
        ((TimelineLogEvent) result.event()).getMessage());
    verify(stepInstanceDao).getStepInstanceView(INLINE_ID, 1L, "generate");
  }

  @Test
  public void testPrepareWithoutPublishedExpansion() {
    artifact.setPrepareInstanceId(1L);
    mockPrepareInstance(WorkflowInstance.Status.SUCCEEDED);
    mockGeneratorView(buildParam(Constants.TEMPLATE_EXPANSION_PARAM, ""));
    AssertHelper.assertThrows(
        "generator did not publish the expansion",
        IllegalArgumentException.class,
        "generator step [generate] published no expansion",
        () -> preparer.prepare(context));
  }

  @Test
  public void testPrepareWithInvalidExpansion() {
    artifact.setPrepareInstanceId(1L);
    mockPrepareInstance(WorkflowInstance.Status.SUCCEEDED);
    mockGeneratorView(buildParam(Constants.TEMPLATE_EXPANSION_PARAM, "not json"));
    AssertHelper.assertThrows(
        "generator published an invalid expansion",
        MaestroInternalError.class,
        "generator step [generate] published an invalid expansion",
        () -> preparer.prepare(context));
  }

  private WorkflowInstance mockPrepareInstance(WorkflowInstance.Status status) {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId(INLINE_ID);
    instance.setWorkflowInstanceId(1L);
    instance.setWorkflowRunId(1L);
    instance.setStatus(status);
    when(instanceDao.getWorkflowInstanceRun(INLINE_ID, 1L, 1L)).thenReturn(instance);
    return instance;
  }

  private void mockGeneratorView(Parameter expansion) {
    StepInstance view = new StepInstance();
    view.setParams(Map.of(Constants.TEMPLATE_EXPANSION_PARAM, expansion));
    when(stepInstanceDao.getStepInstanceView(INLINE_ID, 1L, "generate")).thenReturn(view);
  }
}
