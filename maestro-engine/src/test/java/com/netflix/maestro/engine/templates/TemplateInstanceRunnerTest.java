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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class TemplateInstanceRunnerTest extends MaestroBaseTest {
  @Mock private WorkflowActionHandler actionHandler;
  @Mock private InstanceStepConcurrencyHandler concurrencyHandler;

  private TemplateInstanceRunner runner;
  private WorkflowSummary workflowSummary;
  private StepRuntimeSummary runtimeSummary;
  private TemplateStep step;
  private TemplateArtifact artifact;
  private Workflow inlineWorkflow;
  private Map<String, ParamDefinition> runParams;

  @Before
  public void setUp() {
    runner = new TemplateInstanceRunner(actionHandler, concurrencyHandler);
    workflowSummary = new WorkflowSummary();
    workflowSummary.setWorkflowId("test-workflow");
    workflowSummary.setWorkflowInstanceId(1L);
    workflowSummary.setWorkflowRunId(1L);
    workflowSummary.setInternalId(12345L);
    workflowSummary.setWorkflowVersionId(2L);
    workflowSummary.setInitiator(new ManualInitiator());
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    workflowSummary.setRunProperties(new RunProperties());
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    runtimeSummary =
        StepRuntimeSummary.builder()
            .stepId("publish")
            .stepAttemptId(1L)
            .type(StepType.TEMPLATE)
            .stepRetry(stepRetry)
            .build();
    step = new TemplateStep();
    step.setId("publish");
    artifact = new TemplateArtifact();
    artifact.setTemplateWorkflowId("maestro_template_7D3_11_abc");
    artifact.setTemplateRunId(3L);
    artifact.setTemplateInstanceId(2L);
    inlineWorkflow =
        TemplateInstanceRunner.createInlineWorkflow(
            artifact.getTemplateWorkflowId(), "[identity]", step, List.of());
    runParams = Map.of("foo", ParamDefinition.buildParamDefinition("foo", "bar"));
    when(concurrencyHandler.addInstance(any(RunRequest.class))).thenReturn(true);
    when(actionHandler.runTemplateInstance(
            any(), any(), anyLong(), any(), any(), anyLong(), anyLong(), any()))
        .thenReturn(Optional.empty());
  }

  @Test
  public void testCreateInlineWorkflow() {
    assertEquals("maestro_template_7D3_11_abc", inlineWorkflow.getId());
    assertEquals("[identity]", inlineWorkflow.getName());
    assertTrue(inlineWorkflow.getParams().isEmpty());
    assertTrue(inlineWorkflow.getSteps().isEmpty());
  }

  @Test
  public void testRunFreshExecutionInstance() {
    Optional<Details> details =
        runner.run(
            workflowSummary, runtimeSummary, step, artifact, 2L, inlineWorkflow, runParams, false);

    assertFalse(details.isPresent());
    assertNotNull(artifact.getTemplateUuid());
    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler)
        .runTemplateInstance(
            eq(inlineWorkflow),
            eq(12345L),
            eq(2L),
            eq(workflowSummary.getRunProperties()),
            eq("publish"),
            eq(2L),
            eq(3L),
            captor.capture());
    RunRequest request = captor.getValue();
    assertTrue(request.isFreshRun());
    assertEquals(RunPolicy.START_FRESH_NEW_RUN, request.getCurrentPolicy());
    assertEquals(Initiator.Type.TEMPLATE, request.getInitiator().getType());
    assertEquals("publish", request.getInitiator().getParent().getStepId());
    assertEquals(runParams, request.getRunParams());
    assertEquals(
        List.of("maestro_template"), request.getRuntimeTags().stream().map(Tag::getName).toList());
    assertEquals(artifact.getTemplateUuid(), request.getRequestId().toString());
  }

  @Test
  public void testRunPrepareInstanceKeepsTemplateUuidUnset() {
    runner.run(
        workflowSummary, runtimeSummary, step, artifact, 1L, inlineWorkflow, runParams, false);
    assertNull(artifact.getTemplateUuid());
    verify(actionHandler)
        .runTemplateInstance(any(), any(), anyLong(), any(), any(), eq(1L), eq(3L), any());
  }

  @Test
  public void testRunRestartsPreviousRun() {
    runner.run(
        workflowSummary, runtimeSummary, step, artifact, 2L, inlineWorkflow, runParams, true);
    ArgumentCaptor<RunRequest> captor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler)
        .runTemplateInstance(
            any(), any(), anyLong(), any(), any(), eq(2L), eq(3L), captor.capture());
    assertFalse(captor.getValue().isFreshRun());
    assertEquals(RunPolicy.RESTART_FROM_INCOMPLETE, captor.getValue().getCurrentPolicy());
  }

  @Test
  public void testRunUnavailableDueToInstanceStepConcurrency() {
    when(concurrencyHandler.addInstance(any(RunRequest.class))).thenReturn(false);
    Optional<Details> details =
        runner.run(
            workflowSummary, runtimeSummary, step, artifact, 2L, inlineWorkflow, runParams, false);
    assertTrue(details.isPresent());
    assertTrue(details.get().getMessage().contains("InstanceStepConcurrency"));
    verify(actionHandler, never())
        .runTemplateInstance(any(), any(), anyLong(), any(), any(), anyLong(), anyLong(), any());
  }

  @Test
  public void testRunReturnsHandlerDetails() {
    when(actionHandler.runTemplateInstance(
            any(), any(), anyLong(), any(), any(), anyLong(), anyLong(), any()))
        .thenReturn(Optional.of(Details.create("failed to insert")));
    Optional<Details> details =
        runner.run(
            workflowSummary, runtimeSummary, step, artifact, 2L, inlineWorkflow, runParams, false);
    assertEquals("failed to insert", details.orElseThrow().getMessage());
  }
}
