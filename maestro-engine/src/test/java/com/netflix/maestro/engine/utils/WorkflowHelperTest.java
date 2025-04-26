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
import static org.mockito.ArgumentMatchers.any;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.eval.MaestroParamExtensionRepo;
import com.netflix.maestro.engine.eval.ParamEvaluator;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.params.ParamsManager;
import com.netflix.maestro.engine.transformation.DagTranslator;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.TimeInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.RunProperties;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class WorkflowHelperTest extends MaestroEngineBaseTest {
  @Mock private ParamsManager paramsManager;
  @Mock private ParamEvaluator paramEvaluator;
  @Mock private DagTranslator dagTranslator;
  @Mock private MaestroParamExtensionRepo extensionRepo;
  private WorkflowHelper workflowHelper;
  private WorkflowDefinition definition;
  private WorkflowInstance instance;

  @Before
  public void setup() throws IOException {
    workflowHelper =
        new WorkflowHelper(paramsManager, paramEvaluator, dagTranslator, extensionRepo, 10);
    definition =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf.json", WorkflowDefinition.class);
    definition.getMetadata().setWorkflowVersionId(1L);
    instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setWorkflowId("sample-minimal-wf");
    instance.setRuntimeWorkflow(definition.getWorkflow());
    instance.setInternalId(12345L);
  }

  @Test
  public void testGetRunStrategy() throws IOException {
    assertNull(definition.getPropertiesSnapshot().getRunStrategy());
    assertEquals(RunStrategy.Rule.SEQUENTIAL, definition.getRunStrategyOrDefault().getRule());
    assertEquals(Defaults.DEFAULT_RUN_STRATEGY, definition.getRunStrategyOrDefault());
    WorkflowDefinition sequential =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf-run-strategy-sequential.json",
            WorkflowDefinition.class);
    assertEquals(
        RunStrategy.Rule.SEQUENTIAL, sequential.getPropertiesSnapshot().getRunStrategy().getRule());
    WorkflowDefinition parallel =
        loadObject(
            "fixtures/workflows/definition/sample-minimal-wf-run-strategy-parallel.json",
            WorkflowDefinition.class);
    assertEquals(
        RunStrategy.Rule.PARALLEL, parallel.getPropertiesSnapshot().getRunStrategy().getRule());
  }

  @Test
  public void testCreateWorkflowInstance() {
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Workflow workflow = definition.getWorkflow();
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertEquals(workflow.getId(), instance.getWorkflowId());
    assertEquals(WorkflowInstance.Status.CREATED, instance.getStatus());
    assertEquals(12345L, instance.getInternalId().longValue());
    assertNotNull(instance.getParams());
    assertNotNull(instance.getWorkflowUuid());
    assertEquals(10, instance.getGroupInfo());
    Mockito.verify(paramsManager, Mockito.times(1)).generateMergedWorkflowParams(any(), any());
  }

  @Test
  public void testCreateWorkflowInstanceWithOverriddenWorkflowConfig() {
    ForeachInitiator initiator = new ForeachInitiator();
    initiator.setAncestors(Collections.singletonList(new UpstreamInitiator.Info()));
    RunRequest request =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .groupInfo(8L)
            .build();
    Workflow workflow = definition.getWorkflow().toBuilder().instanceStepConcurrency(20L).build();
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertNull(instance.getRuntimeWorkflow().getInstanceStepConcurrency());
    assertEquals(12345L, instance.getInternalId().longValue());
    assertEquals(8, instance.getGroupInfo());

    request =
        RunRequest.builder()
            .initiator(initiator)
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .instanceStepConcurrency(15L)
            .build();
    instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertEquals(15, instance.getRuntimeWorkflow().getInstanceStepConcurrency().longValue());
    assertEquals(12345L, instance.getInternalId().longValue());

    request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertEquals(20, instance.getRuntimeWorkflow().getInstanceStepConcurrency().longValue());
    assertEquals(12345L, instance.getInternalId().longValue());

    request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .instanceStepConcurrency(15L)
            .build();
    instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertEquals(15, instance.getRuntimeWorkflow().getInstanceStepConcurrency().longValue());
    assertEquals(12345L, instance.getInternalId().longValue());
  }

  @Test
  public void testCreateWorkflowInstanceWithInstanceStepConcurrencyOverride() {
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .instanceStepConcurrency(10L)
            .build();
    Workflow workflow = definition.getWorkflow();
    workflow = workflow.toBuilder().instanceStepConcurrency(20L).build();
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertEquals(workflow.getId(), instance.getWorkflowId());
    assertEquals(WorkflowInstance.Status.CREATED, instance.getStatus());
    assertNotNull(instance.getParams());
    assertNotNull(instance.getWorkflowUuid());
    assertEquals(10L, instance.getRuntimeWorkflow().getInstanceStepConcurrency().longValue());
    assertEquals(12345L, instance.getInternalId().longValue());
    Mockito.verify(paramsManager, Mockito.times(1)).generateMergedWorkflowParams(any(), any());
  }

  @Test
  public void testCreateWorkflowInstanceRestart() {
    RunRequest startRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .build();
    Workflow workflow = definition.getWorkflow();
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(
            workflow, 12345L, 1, new RunProperties(), startRequest);

    RunRequest request =
        RunRequest.builder()
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .initiator(new ManualInitiator())
            .build();
    workflowHelper.updateWorkflowInstance(instance, request);

    assertEquals(workflow.getId(), instance.getWorkflowId());
    assertEquals(WorkflowInstance.Status.CREATED, instance.getStatus());
    assertNotNull(instance.getParams());
    assertNotNull(instance.getWorkflowUuid());
    assertNotNull(instance.getAggregatedInfo());
    assertEquals(1, instance.getAggregatedInfo().getStepAggregatedViews().size());
    assertEquals(
        WorkflowInstance.Status.CREATED, instance.getAggregatedInfo().getWorkflowInstanceStatus());
    assertEquals(12345L, instance.getInternalId().longValue());
    Mockito.verify(paramsManager, Mockito.times(2)).generateMergedWorkflowParams(any(), any());
  }

  @Test
  public void testCreateWorkflowInstanceFailedInitialization() {
    RunRequest badStartRequest =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .runParams(
                Collections.singletonMap(
                    "owner", ParamDefinition.buildParamDefinition("owner", "tester")))
            .build();
    Workflow workflow = definition.getWorkflow();
    Mockito.when(paramsManager.generateMergedWorkflowParams(any(), any()))
        .thenThrow(new MaestroInternalError("test-error"));

    AssertHelper.assertThrows(
        "should throw MaestroInternalError for initialization failure",
        MaestroInternalError.class,
        "test-error",
        () ->
            workflowHelper.createWorkflowInstance(
                workflow, 12345L, 1, new RunProperties(), badStartRequest));

    RunRequest startRequest = badStartRequest.toBuilder().persistFailedRun(true).build();
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(
            workflow, 12345L, 1, new RunProperties(), startRequest);
    assertEquals(workflow.getId(), instance.getWorkflowId());
    assertEquals(WorkflowInstance.Status.FAILED, instance.getStatus());
    assertEquals(12345L, instance.getInternalId().longValue());
    Mockito.verify(paramsManager, Mockito.times(2)).generateMergedWorkflowParams(any(), any());
  }

  @Test
  public void testCreateWorkflowSummaryFromInstance() {
    WorkflowSummary summary = workflowHelper.createWorkflowSummaryFromInstance(instance);
    assertEquals(instance.getWorkflowId(), summary.getWorkflowId());
    assertEquals(instance.getWorkflowVersionId(), summary.getWorkflowVersionId());
    assertEquals(instance.getCreateTime(), summary.getCreationTime());
    assertEquals(instance.getInternalId(), summary.getInternalId());
    assertEquals(instance.getGroupInfo(), summary.getGroupInfo());
    assertEquals(definition.getWorkflow().getDag().keySet(), summary.getStepMap().keySet());
  }

  @Test
  public void testCreateWorkflowSummaryFromInstanceWithInstanceStepConcurrency() {
    WorkflowSummary summary = workflowHelper.createWorkflowSummaryFromInstance(instance);
    assertEquals(instance.getWorkflowId(), summary.getWorkflowId());
    assertEquals(instance.getWorkflowVersionId(), summary.getWorkflowVersionId());
    assertEquals(instance.getCreateTime(), summary.getCreationTime());
    assertEquals(instance.getInternalId(), summary.getInternalId());
    assertNull(summary.getInstanceStepConcurrency());

    instance.getRunProperties().setStepConcurrency(10L);
    instance.setGroupInfo(8L);
    summary = workflowHelper.createWorkflowSummaryFromInstance(instance);
    assertNull(summary.getInstanceStepConcurrency());
    assertEquals(8, summary.getGroupInfo());

    // use runtime workflow instance_step_concurrency
    instance.setRuntimeWorkflow(
        instance.getRuntimeWorkflow().toBuilder().instanceStepConcurrency(20L).build());
    summary = workflowHelper.createWorkflowSummaryFromInstance(instance);
    assertEquals(20L, summary.getInstanceStepConcurrency().longValue());
  }

  @Test
  public void testUpdateWorkflowInstance() {
    Map<String, ParamDefinition> runParams =
        Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1"));
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .runParams(runParams)
            .build();
    Workflow workflow = definition.getWorkflow();
    WorkflowInstance instance =
        workflowHelper.createWorkflowInstance(workflow, 12345L, 1, new RunProperties(), request);
    assertEquals(workflow.getId(), instance.getWorkflowId());
    assertEquals(WorkflowInstance.Status.CREATED, instance.getStatus());
    assertEquals(12345L, instance.getInternalId().longValue());
    assertNotNull(instance.getParams());
    assertNotNull(instance.getWorkflowUuid());
    // For manual initiator, verify if params are generated.
    Mockito.verify(paramsManager, Mockito.times(1)).generateMergedWorkflowParams(instance, request);
    request =
        RunRequest.builder()
            .initiator(new TimeInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .runParams(runParams)
            .build();
    WorkflowInstance createdInstance =
        workflowHelper.createWorkflowInstance(workflow, 123L, 1, new RunProperties(), request);
    // For trigger based, verify if null has been passed.
    Mockito.verify(paramsManager, Mockito.times(1))
        .generateMergedWorkflowParams(createdInstance, request);
  }

  @Test
  public void testRestartCreatedWithAggregatedView() {
    RunRequest request =
        RunRequest.builder()
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .initiator(new ManualInitiator())
            .requestId(UUID.fromString("41f0281e-41a2-468d-b830-56141b2f768b"))
            .build();
    instance.setStatus(WorkflowInstance.Status.FAILED);
    workflowHelper.updateWorkflowInstance(instance, request);
    assertEquals(WorkflowInstance.Status.CREATED, instance.getStatus());
    assertEquals(WorkflowInstance.Status.CREATED, instance.getRunStatus());
    assertEquals(
        Collections.singletonMap(
            "job1", StepAggregatedView.builder().status(StepInstance.Status.NOT_CREATED).build()),
        instance.getAggregatedInfo().getStepAggregatedViews());
    assertEquals(
        WorkflowInstance.Status.FAILED, instance.getAggregatedInfo().getWorkflowInstanceStatus());
  }
}
