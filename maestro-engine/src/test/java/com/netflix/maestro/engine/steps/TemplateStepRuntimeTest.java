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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.concurrency.InstanceStepConcurrencyHandler;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.handlers.WorkflowActionHandler;
import com.netflix.maestro.engine.templates.JobTemplateManager;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Tag;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.initiator.Initiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.InstanceActionJobEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class TemplateStepRuntimeTest extends MaestroBaseTest {
  private static final String WORKFLOW_ID = "test-workflow";
  private static final String STEP_ID = "publish";
  private static final String JOB_TYPE = "write_audit_publish";
  private static final String VERSION = "v3";

  @Mock private WorkflowActionHandler actionHandler;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private InstanceStepConcurrencyHandler concurrencyHandler;
  @Mock private JobTemplateManager jobTemplateManager;

  private TemplateStepRuntime templateStepRuntime;
  private WorkflowSummary workflowSummary;
  private TemplateStep step;

  @Before
  public void setUp() {
    templateStepRuntime =
        new TemplateStepRuntime(
            actionHandler,
            instanceDao,
            stepInstanceDao,
            queueSystem,
            concurrencyHandler,
            jobTemplateManager);

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

    step = new TemplateStep();
    step.setId(STEP_ID);
    step.setSubType(JOB_TYPE);
    step.setSubTypeVersion(VERSION);
    step.setParams(Map.of("target_table", buildParam("target_table", "core.trips").toDefinition()));

    TypedStep bodyStep = new TypedStep();
    bodyStep.setId("write");
    bodyStep.setType(StepType.NOOP);
    when(jobTemplateManager.getJobTemplateVersion(workflowSummary, step)).thenReturn(VERSION);
    when(jobTemplateManager.loadSteps(step, VERSION)).thenReturn(List.<Step>of(bodyStep));
    when(concurrencyHandler.addInstance(any(RunRequest.class))).thenReturn(true);
  }

  private StepRuntimeSummary createStepRuntimeSummary(Map<String, Artifact> artifacts) {
    Map<String, Parameter> params = new HashMap<>();
    params.put("target_table", buildParam("target_table", "core.trips"));
    StepInstance.StepRetry stepRetry = new StepInstance.StepRetry();
    stepRetry.setRetryable(true);
    return StepRuntimeSummary.builder()
        .stepId(STEP_ID)
        .stepAttemptId(1L)
        .type(StepType.TEMPLATE)
        .artifacts(artifacts)
        .params(params)
        .stepRetry(stepRetry)
        .build();
  }

  private TemplateArtifact createArtifact(long runId) {
    TemplateArtifact artifact = new TemplateArtifact();
    artifact.setTemplateWorkflowId("maestro_template_7D3_11_abc");
    artifact.setTemplateInstanceId(1L);
    artifact.setTemplateRunId(runId);
    artifact.setTemplateUuid("template-uuid");
    artifact.setJobType(JOB_TYPE);
    artifact.setTemplateVersion(VERSION);
    return artifact;
  }

  @Test
  public void testStartFreshRun() {
    when(actionHandler.runTemplateInstance(any(), any(), anyLong(), any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    TemplateArtifact artifact = result.artifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
    assertTrue(artifact.getTemplateWorkflowId().startsWith("maestro_template_7D3_11_"));
    assertEquals(1L, artifact.getTemplateInstanceId());
    assertEquals(1L, artifact.getTemplateRunId());
    assertNotNull(artifact.getTemplateUuid());
    assertEquals(JOB_TYPE, artifact.getJobType());
    assertEquals(VERSION, artifact.getTemplateVersion());
    assertEquals(
        "Started a template inline workflow instance: " + artifact.getIdentity(),
        ((TimelineLogEvent) result.timeline().getFirst()).getMessage());

    ArgumentCaptor<Workflow> workflowCaptor = ArgumentCaptor.forClass(Workflow.class);
    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler, times(1))
        .runTemplateInstance(
            workflowCaptor.capture(),
            eq(12345L),
            eq(2L),
            eq(workflowSummary.getRunProperties()),
            eq(STEP_ID),
            eq(artifact),
            requestCaptor.capture());
    Workflow inlineWorkflow = workflowCaptor.getValue();
    assertEquals(artifact.getTemplateWorkflowId(), inlineWorkflow.getId());
    assertEquals(List.of("write"), inlineWorkflow.getAllStepIds());
    assertTrue(inlineWorkflow.getParams().isEmpty());

    RunRequest runRequest = requestCaptor.getValue();
    assertTrue(runRequest.isFreshRun());
    assertEquals(Initiator.Type.TEMPLATE, runRequest.getInitiator().getType());
    assertEquals(WORKFLOW_ID, runRequest.getInitiator().getParent().getWorkflowId());
    assertEquals(STEP_ID, runRequest.getInitiator().getParent().getStepId());
    Map<String, ParamDefinition> runParams = runRequest.getRunParams();
    assertEquals(2, runParams.size());
    assertEquals("wf_value", runParams.get("wf_param").asStringParamDef().getValue());
    assertEquals("core.trips", runParams.get("target_table").asStringParamDef().getValue());
    assertEquals(
        List.of("maestro_template"),
        runRequest.getRuntimeTags().stream().map(Tag::getName).toList());
    verify(stepInstanceDao, never()).getLatestTemplateArtifact(anyString(), anyLong(), anyString());
  }

  @Test
  public void testStartWithPreviousArtifact() {
    when(actionHandler.runTemplateInstance(any(), any(), anyLong(), any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    when(stepInstanceDao.getLatestTemplateArtifact(WORKFLOW_ID, 1L, STEP_ID))
        .thenReturn(createArtifact(2L));
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    TemplateArtifact artifact = result.artifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
    assertEquals(1L, artifact.getTemplateInstanceId());
    assertEquals(3L, artifact.getTemplateRunId());

    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler, times(1))
        .runTemplateInstance(
            any(), any(), anyLong(), any(), any(), eq(artifact), requestCaptor.capture());
    assertFalse(requestCaptor.getValue().isFreshRun());
    assertEquals(RunPolicy.RESTART_FROM_INCOMPLETE, requestCaptor.getValue().getCurrentPolicy());
  }

  @Test
  public void testStartWithoutPreviousArtifactOnRestart() {
    when(actionHandler.runTemplateInstance(any(), any(), anyLong(), any(), any(), any(), any()))
        .thenReturn(Optional.empty());
    workflowSummary.setRunPolicy(RunPolicy.RESTART_FROM_INCOMPLETE);
    when(stepInstanceDao.getLatestTemplateArtifact(WORKFLOW_ID, 1L, STEP_ID)).thenReturn(null);
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    TemplateArtifact artifact = result.artifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
    assertEquals(1L, artifact.getTemplateRunId());
    ArgumentCaptor<RunRequest> requestCaptor = ArgumentCaptor.forClass(RunRequest.class);
    verify(actionHandler, times(1))
        .runTemplateInstance(
            any(), any(), anyLong(), any(), any(), eq(artifact), requestCaptor.capture());
    assertEquals(RunPolicy.START_FRESH_NEW_RUN, requestCaptor.getValue().getCurrentPolicy());
  }

  @Test
  public void testStartRetryLaterWhenRunFails() {
    when(actionHandler.runTemplateInstance(any(), any(), anyLong(), any(), any(), any(), any()))
        .thenReturn(Optional.of(Details.create("failed to insert")));
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
  }

  @Test
  public void testStartUnavailableDueToInstanceStepConcurrency() {
    when(concurrencyHandler.addInstance(any(RunRequest.class))).thenReturn(false);
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.CONTINUE, result.state());
    assertTrue(result.artifacts().isEmpty());
    verify(actionHandler, never())
        .runTemplateInstance(any(), any(), anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testStartFatalErrorWhenTemplateMissing() {
    when(jobTemplateManager.loadSteps(step, VERSION))
        .thenThrow(new NullPointerException("Cannot find the job template"));
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

    assertEquals(StepRuntime.State.FATAL_ERROR, result.state());
    assertTrue(result.artifacts().isEmpty());
  }

  @Test
  public void testTrackTemplateInstance() {
    Map<WorkflowInstance.Status, StepRuntime.State> expected = new HashMap<>();
    expected.put(WorkflowInstance.Status.CREATED, StepRuntime.State.CONTINUE);
    expected.put(WorkflowInstance.Status.IN_PROGRESS, StepRuntime.State.CONTINUE);
    expected.put(WorkflowInstance.Status.PAUSED, StepRuntime.State.CONTINUE);
    expected.put(WorkflowInstance.Status.SUCCEEDED, StepRuntime.State.DONE);
    expected.put(WorkflowInstance.Status.FAILED, StepRuntime.State.FATAL_ERROR);
    expected.put(WorkflowInstance.Status.STOPPED, StepRuntime.State.STOPPED);
    expected.put(WorkflowInstance.Status.TIMED_OUT, StepRuntime.State.TIMED_OUT);

    expected.forEach(
        (status, state) -> {
          TemplateArtifact artifact = createArtifact(1L);
          WorkflowInstance instance = new WorkflowInstance();
          instance.setStatus(status);
          WorkflowRuntimeOverview overview = new WorkflowRuntimeOverview();
          overview.setTotalStepCount(3);
          instance.setRuntimeOverview(overview);
          when(instanceDao.getWorkflowInstanceRun("maestro_template_7D3_11_abc", 1L, 1L))
              .thenReturn(instance);
          StepRuntimeSummary runtimeSummary =
              createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));

          StepRuntime.Result result =
              templateStepRuntime.execute(workflowSummary, step, runtimeSummary);

          assertEquals(state, result.state());
          TemplateArtifact updated =
              result.artifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate();
          assertEquals(overview, updated.getTemplateOverview());
          assertEquals(status.isTerminal() ? 1 : 0, result.timeline().size());
        });
  }

  @Test
  public void testTerminateWithoutArtifact() {
    StepRuntimeSummary runtimeSummary = createStepRuntimeSummary(new HashMap<>());

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertTrue(result.artifacts().isEmpty());
  }

  @Test
  public void testTerminateWithWakeUpUnderlyingActor() {
    workflowSummary.setGroupInfo(5L);
    TemplateArtifact artifact = createArtifact(3L);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus("maestro_template_7D3_11_abc", 1L, 3L))
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
                  if (!msg.msgId().equals("[FLOW][maestro_template_7D3_11_abc]1")) {
                    return false;
                  }
                  if (msg.event() instanceof InstanceActionJobEvent event) {
                    return event.getWorkflowId().equals("maestro_template_7D3_11_abc")
                        && event.getGroupInfo() == 5L
                        && event.getInstanceRunIds().size() == 1
                        && event.getInstanceRunIds().get(1L) == 3L;
                  }
                  return false;
                }));
  }

  @Test
  public void testTerminateTerminatedInstance() {
    TemplateArtifact artifact = createArtifact(1L);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus("maestro_template_7D3_11_abc", 1L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    WorkflowInstance instance = new WorkflowInstance();
    instance.setStatus(WorkflowInstance.Status.STOPPED);
    instance.setRuntimeOverview(new WorkflowRuntimeOverview());
    when(instanceDao.getWorkflowInstanceRun("maestro_template_7D3_11_abc", 1L, 1L))
        .thenReturn(instance);

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertEquals(
        instance.getRuntimeOverview(),
        result.artifacts().get(Artifact.Type.TEMPLATE.key()).asTemplate().getTemplateOverview());
  }

  @Test
  public void testTerminateNotFoundInstance() {
    TemplateArtifact artifact = createArtifact(1L);
    StepRuntimeSummary runtimeSummary =
        createStepRuntimeSummary(Map.of(Artifact.Type.TEMPLATE.key(), artifact));
    when(instanceDao.getWorkflowInstanceStatus("maestro_template_7D3_11_abc", 1L, 1L))
        .thenReturn(null);

    StepRuntime.Result result = templateStepRuntime.terminate(workflowSummary, runtimeSummary);

    assertEquals(StepRuntime.State.STOPPED, result.state());
    assertTrue(result.artifacts().isEmpty());
    assertEquals(1, result.timeline().size());
  }

  @Test
  public void testInjectRuntimeParamsAndTags() {
    Map<String, ParamDefinition> params =
        Map.of("target_table", buildParam("target_table", "core.trips").toDefinition());
    when(jobTemplateManager.loadRuntimeParams(workflowSummary, step)).thenReturn(params);
    when(jobTemplateManager.loadTags(workflowSummary, step)).thenReturn(List.of(Tag.create("foo")));

    assertEquals(params, templateStepRuntime.injectRuntimeParams(workflowSummary, step));
    assertEquals(
        List.of(Tag.create("foo")), templateStepRuntime.injectRuntimeTags(workflowSummary, step));
  }
}
