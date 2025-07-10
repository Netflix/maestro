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
package com.netflix.maestro.engine.processors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.artifact.Artifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.jobevents.InstanceActionJobEvent;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class InstanceActionJobEventProcessorTest extends MaestroEngineBaseTest {
  @Mock private FlowOperation flowOperation;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private StepInstance stepInstance;
  @Mock private Step stepDefinition;
  @Mock private StepRuntimeState stepRuntimeState;
  @Mock private Map<String, Artifact> artifactMap;
  @Mock private StepInstance foreachStepInstance;
  @Mock private WorkflowInstance workflowInstance;

  private InstanceActionJobEvent event;
  private final String workflowId = "sample-test-workflow-id";
  private final long workflowInstanceId = 2;
  private final long workflowRunId = 3;
  private final String stepAttemptId = Constants.LATEST_INSTANCE_RUN;
  private final long groupInfo = 12L;
  private final String stepId = "sample-test-step-id";
  private InstanceActionJobEventProcessor processor;

  @Before
  public void before() {
    processor = new InstanceActionJobEventProcessor(flowOperation, instanceDao, stepInstanceDao);
    event = new InstanceActionJobEvent();
    event.setWorkflowId(workflowId);
    event.setWorkflowInstanceId(workflowInstanceId);
    event.setWorkflowRunId(workflowRunId);
    event.setStepId(stepId);
    event.setStepAttemptId(stepAttemptId);
    event.setGroupInfo(groupInfo);
    when(stepInstanceDao.getStepInstance(
            workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId))
        .thenReturn(stepInstance);
    String foreachWorkflowId = "sample-test-foreach-workflow-id";
    String foreachWorkflowInstanceId = "1";
    String foreachWorkflowRunId = "4";
    String foreachStepAttemptId = "1";
    String foreachStepId = "sample-test-foreach-step-id";
    when(stepInstanceDao.getStepInstance(
            foreachWorkflowId,
            Long.parseLong(foreachWorkflowInstanceId),
            Long.parseLong(foreachWorkflowRunId),
            foreachStepId,
            foreachStepAttemptId))
        .thenReturn(foreachStepInstance);
    when(instanceDao.getWorkflowInstanceRun(workflowId, workflowInstanceId, workflowRunId))
        .thenReturn(workflowInstance);
    setupInstanceBase();
    when(flowOperation.wakeUp(anyLong(), any())).thenReturn(true);
    when(flowOperation.wakeUp(anyLong(), any(), any())).thenReturn(true);
  }

  private void setupInstanceBase() {
    when(stepInstance.getDefinition()).thenReturn(stepDefinition);
    when(stepInstance.getRuntimeState()).thenReturn(stepRuntimeState);
    when(stepInstance.getArtifacts()).thenReturn(artifactMap);
    when(stepInstance.getStepId()).thenReturn(stepId);
    when(stepInstance.getGroupInfo()).thenReturn(groupInfo);
    when(stepInstance.getStepAttemptId()).thenReturn(2L);
    when(foreachStepInstance.getGroupInfo()).thenReturn(groupInfo);
  }

  private void setStepInstanceDefinition(StepType type) {
    when(stepDefinition.getType()).thenReturn(type);
  }

  private void setStepInstanceRuntimeState(StepInstance.Status status) {
    when(stepRuntimeState.getStatus()).thenReturn(status);
  }

  @Test
  public void testStepNonRetryable() {
    event.setEntityType(InstanceActionJobEvent.EntityType.STEP);
    setStepInstanceRuntimeState(StepInstance.Status.SKIPPED);
    Assert.assertTrue(processor.process(event).isEmpty());
    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(flowOperation);
    Mockito.verifyNoInteractions(instanceDao);
  }

  @Test
  public void testStepRetryable() {
    event.setEntityType(InstanceActionJobEvent.EntityType.STEP);
    setStepInstanceRuntimeState(StepInstance.Status.FATALLY_FAILED);
    event.setStepAction(Actions.StepInstanceAction.RESTART);
    Assert.assertTrue(processor.process(event).isEmpty());

    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(2, "[sample-test-workflow-id][2][3]", stepId);
    Mockito.verifyNoInteractions(stepInstanceDao);
    Mockito.verifyNoInteractions(instanceDao);

    when(flowOperation.wakeUp(anyLong(), any(), any())).thenReturn(false);
    AssertHelper.assertThrows(
        "Underlying task is not woken up and should retry",
        MaestroRetryableError.class,
        "group [2] is not woken up successfully. Will try again",
        () -> processor.process(event));
    Mockito.verify(flowOperation, Mockito.times(2))
        .wakeUp(2, "[sample-test-workflow-id][2][3]", stepId);
  }

  @Test
  public void testStepNotEnoughInfo() {
    event.setEntityType(InstanceActionJobEvent.EntityType.STEP);
    event.setStepAction(Actions.StepInstanceAction.STOP);
    setStepInstanceDefinition(StepType.NOTEBOOK);
    setStepInstanceRuntimeState(StepInstance.Status.PLATFORM_FAILED);
    when(stepInstanceDao.getStepInstance(
            workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId))
        .thenReturn(null);
    Assert.assertTrue(processor.process(event).isEmpty());

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(flowOperation);
    Mockito.verifyNoInteractions(instanceDao);
  }

  @Test
  public void testStepReachDesiredState() {
    event.setEntityType(InstanceActionJobEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.SUBWORKFLOW);
    setStepInstanceRuntimeState(StepInstance.Status.STOPPED);
    event.setStepAction(Actions.StepInstanceAction.STOP);
    Assert.assertTrue(processor.process(event).isEmpty());

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(instanceDao);
    Mockito.verifyNoInteractions(flowOperation);
  }

  @Test
  public void testDefaultNonLeafStepNoAction() {
    event.setEntityType(InstanceActionJobEvent.EntityType.STEP);
    setStepInstanceDefinition(StepType.TEMPLATE);
    setStepInstanceRuntimeState(StepInstance.Status.INTERNALLY_FAILED);
    event.setStepAction(Actions.StepInstanceAction.STOP);
    Assert.assertTrue(processor.process(event).isEmpty());

    Mockito.verify(stepInstanceDao, Mockito.times(1))
        .getStepInstance(workflowId, workflowInstanceId, workflowRunId, stepId, stepAttemptId);
    Mockito.verifyNoInteractions(instanceDao);
    Mockito.verifyNoInteractions(flowOperation);
  }

  @Test
  public void testStepRestartAction() {
    event.setEntityType(InstanceActionJobEvent.EntityType.STEP);
    event.setStepAction(Actions.StepInstanceAction.RESTART);
    Assert.assertTrue(processor.process(event).isEmpty());
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(2, "[sample-test-workflow-id][2][3]", stepId);
    Mockito.verifyNoInteractions(stepInstanceDao);
    Mockito.verifyNoInteractions(instanceDao);
    Mockito.reset(flowOperation);

    when(flowOperation.wakeUp(anyLong(), any(), any())).thenReturn(true);
    setStepInstanceDefinition(StepType.SUBWORKFLOW);
    Assert.assertTrue(processor.process(event).isEmpty());
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(2, "[sample-test-workflow-id][2][3]", stepId);
    Mockito.verifyNoInteractions(stepInstanceDao);
    Mockito.verifyNoInteractions(instanceDao);
  }

  @Test
  public void testWorkflowAction() {
    event.setEntityType(InstanceActionJobEvent.EntityType.WORKFLOW);
    event.setWorkflowAction(Actions.WorkflowInstanceAction.UNBLOCK);
    event.setStepId(null);
    Assert.assertTrue(processor.process(event).isEmpty());

    Mockito.verify(instanceDao, Mockito.times(1))
        .getWorkflowInstanceRun(workflowId, workflowInstanceId, workflowRunId);
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(2, Set.of("[sample-test-workflow-id][2][3]"));
  }

  @Test
  public void testWorkflowActionShouldRetry() {
    event.setEntityType(InstanceActionJobEvent.EntityType.WORKFLOW);
    event.setWorkflowAction(Actions.WorkflowInstanceAction.STOP);
    when(workflowInstance.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    event.setStepId(null);
    AssertHelper.assertThrows(
        "A desired terminal state should retry for additional checking",
        MaestroRetryableError.class,
        "Current status [CREATED] is not the desired status after action is taking. Will check again.",
        () -> processor.process(event));

    Mockito.verify(instanceDao, Mockito.times(1))
        .getWorkflowInstanceRun(workflowId, workflowInstanceId, workflowRunId);
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(2, Set.of("[sample-test-workflow-id][2][3]"));

    when(flowOperation.wakeUp(anyLong(), any())).thenReturn(false);
    AssertHelper.assertThrows(
        "Underlying flow is not woken up and should retry",
        MaestroRetryableError.class,
        "group [2] is not woken up successfully. Will try again",
        () -> processor.process(event));
    Mockito.verify(flowOperation, Mockito.times(2))
        .wakeUp(2, Set.of("[sample-test-workflow-id][2][3]"));
  }

  @Test
  public void testFlowAction() {
    event.setEntityType(InstanceActionJobEvent.EntityType.FLOW);
    event.setInstanceRunIds(Map.of(2L, 1L, 3L, 2L, 1L, 3L));
    Assert.assertTrue(processor.process(event).isEmpty());

    Mockito.verifyNoInteractions(stepInstanceDao);
    Mockito.verifyNoInteractions(instanceDao);
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(1, Set.of("[sample-test-workflow-id][1][3]"));
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(0, Set.of("[sample-test-workflow-id][2][1]"));
    Mockito.verify(flowOperation, Mockito.times(1))
        .wakeUp(8, Set.of("[sample-test-workflow-id][3][2]"));
    Mockito.reset(flowOperation);

    Mockito.when(flowOperation.wakeUp(anyLong(), any())).thenReturn(false);
    Assert.assertTrue(processor.process(event).isEmpty());
    Mockito.verify(flowOperation, Mockito.times(3)).wakeUp(anyLong(), any());
  }
}
