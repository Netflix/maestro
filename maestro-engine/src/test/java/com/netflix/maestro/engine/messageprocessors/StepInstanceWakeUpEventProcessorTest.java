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
package com.netflix.maestro.engine.messageprocessors;

import com.netflix.conductor.core.execution.MaestroWorkflowExecutor;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.jobevents.StepInstanceWakeUpEvent;
import com.netflix.maestro.engine.processors.StepInstanceWakeUpEventProcessor;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.artifact.ForeachArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.ForeachStepOverview;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowRollupOverview;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class StepInstanceWakeUpEventProcessorTest extends MaestroEngineBaseTest {
  @Mock private MaestroWorkflowExecutor workflowExecutor;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroWorkflowInstanceDao workflowInstanceDao;
  @Mock private StepInstance stepInstance;
  @Mock private Step stepDefinition;
  @Mock private StepRuntimeState stepRuntimeState;
  @Mock private Map<String, Artifact> artifactMap;
  @Mock private ForeachArtifact foreachArtifact;
  @Mock private ForeachStepOverview foreachStepOverview;
  @Mock private StepInstance foreachStepInstance;
  @Mock private WorkflowInstance workflowInstance;
  @Mock private WorkflowRuntimeOverview workflowRuntimeOverview;

  private StepInstanceWakeUpEvent event;
  private final String workflowId = "sample-test-workflow-id";
  private final long workflowInstanceId = 2;
  private final long workflowRunId = 3;
  private final String stepAttemptId = "2";
  private final String stepUuid = "sample-test-step-uuid";
  private final String stepId = "sample-test-step-id";
  private StepInstanceWakeUpEventProcessor subject;
  private WorkflowRollupOverview overview;

  private final String foreachWorkflowId = "sample-test-foreach-workflow-id";
  private final String foreachWorkflowInstanceId = "1";
  private final String foreachWorkflowRunId = "4";
  private final String foreachStepAttemptId = "1";
  private final String foreachStepUuid = "sample-test-foreach-step-uuid";
  private final String foreachStepId = "sample-test-foreach-step-id";

  @Before
  public void before() {
    subject =
        new StepInstanceWakeUpEventProcessor(
            workflowExecutor, stepInstanceDao, workflowInstanceDao);
    event = new StepInstanceWakeUpEvent();
    event.setWorkflowId(workflowId);
    event.setWorkflowInstanceId(workflowInstanceId);
    event.setWorkflowRunId(workflowRunId);
    event.setStepId(stepId);
    event.setStepAttemptId(stepAttemptId);
    Mockito.when(
            stepInstanceDao.getStepInstance(
                workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId))
        .thenReturn(stepInstance);
    Mockito.when(
            stepInstanceDao.getStepInstance(
                foreachWorkflowId,
                Long.parseLong(foreachWorkflowInstanceId),
                Long.parseLong(foreachWorkflowRunId),
                foreachStepId,
                foreachStepAttemptId))
        .thenReturn(foreachStepInstance);
    Mockito.when(
            workflowInstanceDao.getWorkflowInstance(
                workflowId, workflowInstanceId, String.valueOf(workflowRunId), false))
        .thenReturn(workflowInstance);
    setupInstanceBase();
  }

  private void setupInstanceBase() {
    Mockito.when(stepInstance.getDefinition()).thenReturn(stepDefinition);
    Mockito.when(stepInstance.getRuntimeState()).thenReturn(stepRuntimeState);
    Mockito.when(stepInstance.getArtifacts()).thenReturn(artifactMap);
    Mockito.when(stepInstance.getStepId()).thenReturn(stepId);
    Mockito.when(stepInstance.getStepUuid()).thenReturn(stepUuid);
    Mockito.when(stepInstance.getStepAttemptId()).thenReturn(Long.valueOf(stepAttemptId));
    Mockito.when(foreachStepInstance.getStepUuid()).thenReturn(foreachStepUuid);
  }

  private void setStepInstanceDefinition(StepType type) {
    Mockito.when(stepDefinition.getType()).thenReturn(type);
  }

  private void setStepInstanceRuntimeState(StepInstance.Status status) {
    Mockito.when(stepRuntimeState.getStatus()).thenReturn(status);
  }

  @Test
  public void testLeafStepNonRetryable() {
    event.setStepType(StepType.NOTEBOOK);
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    event.setStepStatus(StepInstance.Status.FATALLY_FAILED);
    subject.process(() -> event);
    Mockito.verifyNoInteractions(stepInstanceDao);
    Mockito.verifyNoInteractions(workflowExecutor);
    Mockito.verifyNoInteractions(workflowInstanceDao);
  }

  @Test
  public void testLeafStepRetryable() {
    event.setStepType(StepType.NOTEBOOK);
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    event.setStepStatus(StepInstance.Status.PLATFORM_FAILED);
    event.setStepUuid(stepUuid);
    subject.process(() -> event);

    Mockito.verify(workflowExecutor, Mockito.times(1)).resetTaskOffset(stepUuid);
    Mockito.verifyNoInteractions(stepInstanceDao);
    Mockito.verifyNoInteractions(workflowInstanceDao);
  }

  @Test
  public void testLeafStepNotEnoughInfo() {
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.NOTEBOOK);
    setStepInstanceRuntimeState(StepInstance.Status.PLATFORM_FAILED);
    subject.process(() -> event);

    Mockito.verify(workflowExecutor, Mockito.times(1)).resetTaskOffset(stepUuid);
    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(workflowInstanceDao);
  }

  @Test
  public void testNestedStepReachDesiredState() {
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.SUBWORKFLOW);
    setStepInstanceRuntimeState(StepInstance.Status.STOPPED);
    event.setStepAction(Actions.StepInstanceAction.STOP);
    subject.process(() -> event);

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(workflowInstanceDao);
    Mockito.verifyNoInteractions(workflowExecutor);
  }

  @Test
  public void testDefaultNonLeafStepNoAction() {
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.TEMPLATE);
    setStepInstanceRuntimeState(StepInstance.Status.PLATFORM_FAILED);
    event.setStepAction(Actions.StepInstanceAction.STOP);
    subject.process(() -> event);

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(workflowInstanceDao);
    Mockito.verifyNoInteractions(workflowExecutor);
  }

  @Test
  public void testForeachStepNoRetry() {
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    ref.setRef(
        Collections.singletonMap(
            String.join(":", foreachWorkflowId, foreachWorkflowRunId),
            Collections.singletonList(
                String.join(":", foreachWorkflowInstanceId, foreachStepId, foreachStepAttemptId))));
    overview =
        WorkflowRollupOverview.of(
            1, Collections.singletonMap(StepInstance.Status.PLATFORM_FAILED, ref));
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.FOREACH);
    setStepInstanceRuntimeState(StepInstance.Status.RUNNING);
    // this action won't be real but for no retry test.
    event.setStepAction(Actions.StepInstanceAction.BYPASS_STEP_DEPENDENCIES);

    Mockito.when(foreachArtifact.asForeach()).thenReturn(foreachArtifact);
    Mockito.when(artifactMap.get(Artifact.Type.FOREACH.key())).thenReturn(foreachArtifact);
    Mockito.when(artifactMap.containsKey(Artifact.Type.FOREACH.key())).thenReturn(true);
    Mockito.when(foreachArtifact.getForeachOverview()).thenReturn(foreachStepOverview);
    Mockito.when(foreachStepOverview.getOverallRollup()).thenReturn(overview);

    subject.process(() -> event);

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(
            foreachWorkflowId,
            Long.parseLong(foreachWorkflowInstanceId),
            Long.parseLong(foreachWorkflowRunId),
            foreachStepId,
            foreachStepAttemptId);
    Mockito.verifyNoInteractions(workflowInstanceDao);
    Mockito.verify(workflowExecutor, Mockito.times(1)).resetTaskOffset(foreachStepUuid);
  }

  @Test
  public void testForeachStepShouldRetry() {
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    ref.setRef(
        Collections.singletonMap(
            String.join(":", foreachWorkflowId, foreachWorkflowRunId),
            Collections.singletonList(
                String.join(":", foreachWorkflowInstanceId, foreachStepId, foreachStepAttemptId))));
    overview =
        WorkflowRollupOverview.of(
            1, Collections.singletonMap(StepInstance.Status.PLATFORM_FAILED, ref));
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.FOREACH);
    setStepInstanceRuntimeState(StepInstance.Status.RUNNING);
    // this action won't be real but for no retry test.
    event.setStepAction(Actions.StepInstanceAction.SKIP);

    Mockito.when(foreachArtifact.asForeach()).thenReturn(foreachArtifact);
    Mockito.when(artifactMap.get(Artifact.Type.FOREACH.key())).thenReturn(foreachArtifact);
    Mockito.when(artifactMap.containsKey(Artifact.Type.FOREACH.key())).thenReturn(true);
    Mockito.when(foreachArtifact.getForeachOverview()).thenReturn(foreachStepOverview);
    Mockito.when(foreachStepOverview.getOverallRollup()).thenReturn(overview);

    AssertHelper.assertThrows(
        "A desired terminal state should retry for additional checking",
        MaestroRetryableError.class,
        "Current status is not the desired status after action is taking. Will check again",
        () -> subject.process(() -> event));

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(
            foreachWorkflowId,
            Long.parseLong(foreachWorkflowInstanceId),
            Long.parseLong(foreachWorkflowRunId),
            foreachStepId,
            foreachStepAttemptId);
    Mockito.verifyNoInteractions(workflowInstanceDao);
    Mockito.verify(workflowExecutor, Mockito.times(1)).resetTaskOffset(foreachStepUuid);
  }

  @Test
  public void testWorkflowAction() {
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.WORKFLOW);
    event.setWorkflowAction(Actions.WorkflowInstanceAction.UNBLOCK);
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    ref.setRef(
        Collections.singletonMap(
            String.join(":", foreachWorkflowId, foreachWorkflowRunId),
            Collections.singletonList(
                String.join(":", foreachWorkflowInstanceId, foreachStepId, foreachStepAttemptId))));
    overview =
        WorkflowRollupOverview.of(
            1, Collections.singletonMap(StepInstance.Status.PLATFORM_FAILED, ref));

    Mockito.when(workflowInstance.getRuntimeOverview()).thenReturn(workflowRuntimeOverview);
    Mockito.when(workflowRuntimeOverview.getRollupOverview()).thenReturn(overview);
    subject.process(() -> event);

    Mockito.verify(workflowInstanceDao, Mockito.times(1))
        .getWorkflowInstance(workflowId, workflowInstanceId, String.valueOf(workflowRunId), false);
    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(
            foreachWorkflowId,
            Long.parseLong(foreachWorkflowInstanceId),
            Long.parseLong(foreachWorkflowRunId),
            foreachStepId,
            foreachStepAttemptId);
    Mockito.verify(workflowExecutor, Mockito.times(1)).resetTaskOffset(foreachStepUuid);
  }

  @Test
  public void testWorkflowActionShouldRetry() {
    event.setEntityType(StepInstanceWakeUpEvent.EntityType.WORKFLOW);
    event.setWorkflowAction(Actions.WorkflowInstanceAction.STOP);
    WorkflowRollupOverview.CountReference ref = new WorkflowRollupOverview.CountReference();
    ref.setCnt(1);
    ref.setRef(
        Collections.singletonMap(
            String.join(":", foreachWorkflowId, foreachWorkflowRunId),
            Collections.singletonList(
                String.join(":", foreachWorkflowInstanceId, foreachStepId, foreachStepAttemptId))));
    overview =
        WorkflowRollupOverview.of(
            1, Collections.singletonMap(StepInstance.Status.PLATFORM_FAILED, ref));

    Mockito.when(workflowInstance.getRuntimeOverview()).thenReturn(workflowRuntimeOverview);
    Mockito.when(workflowRuntimeOverview.getRollupOverview()).thenReturn(overview);
    AssertHelper.assertThrows(
        "A desired terminal state should retry for additional checking",
        MaestroRetryableError.class,
        "Current status is not the desired status after action is taking. Will check again.",
        () -> subject.process(() -> event));

    Mockito.verify(workflowInstanceDao, Mockito.times(1))
        .getWorkflowInstance(workflowId, workflowInstanceId, String.valueOf(workflowRunId), false);
    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(
            foreachWorkflowId,
            Long.parseLong(foreachWorkflowInstanceId),
            Long.parseLong(foreachWorkflowRunId),
            foreachStepId,
            foreachStepAttemptId);
    Mockito.verify(workflowExecutor, Mockito.times(1)).resetTaskOffset(foreachStepUuid);
  }
}
