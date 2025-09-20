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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroRunStrategyDao;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.RunRequest;
import com.netflix.maestro.engine.execution.RunResponse;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroBadRequestException;
import com.netflix.maestro.exceptions.MaestroInvalidStatusException;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.Defaults;
import com.netflix.maestro.models.definition.RunStrategy;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.definition.WorkflowDefinition;
import com.netflix.maestro.models.initiator.ForeachInitiator;
import com.netflix.maestro.models.initiator.ManualInitiator;
import com.netflix.maestro.models.initiator.SubworkflowInitiator;
import com.netflix.maestro.models.initiator.UpstreamInitiator;
import com.netflix.maestro.models.instance.RestartConfig;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepAggregatedView;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.instance.WorkflowInstanceAggregatedInfo;
import com.netflix.maestro.models.instance.WorkflowRuntimeOverview;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class WorkflowInstanceActionHandlerTest extends MaestroEngineBaseTest {

  @Mock private MaestroWorkflowDao workflowDao;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroRunStrategyDao runStrategyDao;
  @Mock private MaestroStepInstanceActionDao actionDao;
  @Mock private WorkflowInstance instance;
  @Mock private WorkflowRuntimeOverview overview;
  @Mock private Workflow workflow;
  @Mock private WorkflowHelper workflowHelper;
  @Mock private WorkflowDefinition workflowDefinition;

  private WorkflowInstanceActionHandler actionHandler;
  private final User user = User.create("test-caller");

  @Before
  public void before() {
    this.actionHandler =
        new WorkflowInstanceActionHandler(
            workflowDao, instanceDao, runStrategyDao, actionDao, workflowHelper);
    when(instanceDao.getLatestWorkflowInstanceRun("test-workflow", 1L)).thenReturn(instance);
    when(instanceDao.tryTerminateQueuedInstance(any(), any(), any())).thenReturn(true);
    when(instance.getRuntimeOverview()).thenReturn(overview);
    when(instance.getRuntimeWorkflow()).thenReturn(workflow);
    when(instance.getExecutionId()).thenReturn(null);
    when(instance.getWorkflowRunId()).thenReturn(1L);
    when(workflowDao.getRunStrategy("test-workflow")).thenReturn(Defaults.DEFAULT_RUN_STRATEGY);
    when(workflowDao.getWorkflowDefinition(any(), any())).thenReturn(workflowDefinition);
  }

  @Test
  public void testStopLatest() {
    when(instanceDao.tryTerminateQueuedInstance(any(), any(), any())).thenReturn(true);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    boolean res = actionHandler.stopLatest("test-workflow", 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.STOPPED), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance.getExecutionId()).thenReturn("foo");
    res = actionHandler.stopLatest("test-workflow", 1, user).isCompleted();
    assertFalse(res);
    verify(instanceDao, times(2)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.STOPPED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.PAUSED);
    when(instance.getExecutionId()).thenReturn(null);
    res = actionHandler.stopLatest("test-workflow", 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(3)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(2))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.STOPPED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());
  }

  @Test
  public void testKillLatest() {
    when(instanceDao.tryTerminateQueuedInstance(any(), any(), any())).thenReturn(true);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    boolean res = actionHandler.killLatest("test-workflow", 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance.getExecutionId()).thenReturn("foo");
    res = actionHandler.killLatest("test-workflow", 1, user).isCompleted();
    assertFalse(res);
    verify(instanceDao, times(2)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.PAUSED);
    when(instance.getExecutionId()).thenReturn(null);
    res = actionHandler.killLatest("test-workflow", 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(3)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(2))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());
  }

  @Test
  public void testStop() {
    when(instanceDao.tryTerminateQueuedInstance(any(), any(), any())).thenReturn(true);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    boolean res = actionHandler.stop("test-workflow", 1, 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.STOPPED), anyString());
    verify(actionDao, times(0)).terminate(any(), any(), any(), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance.getExecutionId()).thenReturn("foo");
    res = actionHandler.stop("test-workflow", 1, 1, user).isCompleted();
    assertFalse(res);
    verify(instanceDao, times(2)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.STOPPED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.PAUSED);
    when(instance.getExecutionId()).thenReturn(null);
    res = actionHandler.stop("test-workflow", 1, 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(3)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(2))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.STOPPED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());
  }

  @Test
  public void testKill() {
    when(instanceDao.tryTerminateQueuedInstance(any(), any(), any())).thenReturn(true);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    boolean res = actionHandler.kill("test-workflow", 1, 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance.getExecutionId()).thenReturn("foo");
    res = actionHandler.kill("test-workflow", 1, 1, user).isCompleted();
    assertFalse(res);
    verify(instanceDao, times(2)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());

    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.PAUSED);
    when(instance.getExecutionId()).thenReturn(null);
    res = actionHandler.kill("test-workflow", 1, 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(3)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(2))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());
  }

  @Test
  public void testTerminateAfterExecuted() {
    when(instance.getExecutionId()).thenReturn("foo");
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    actionHandler.stopLatest("test-workflow", 1, user);
    actionHandler.killLatest("test-workflow", 1, user);
    actionHandler.stop("test-workflow", 1, 1, user);
    actionHandler.kill("test-workflow", 1, 1, user);
    verify(actionDao, times(4)).terminate(any(), any(), any(), anyString());
    verify(instanceDao, times(0)).tryTerminateQueuedInstance(any(), any(), any());
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.PAUSED);
    actionHandler.stopLatest("test-workflow", 1, user);
    actionHandler.killLatest("test-workflow", 1, user);
    actionHandler.stop("test-workflow", 1, 1, user);
    actionHandler.kill("test-workflow", 1, 1, user);
    verify(actionDao, times(8)).terminate(any(), any(), any(), anyString());
    verify(instanceDao, times(0)).tryTerminateQueuedInstance(any(), any(), any());
  }

  @Test
  public void testInvalidRunId() {
    when(instance.getWorkflowRunId()).thenReturn(2L);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    AssertHelper.assertThrows(
        "run id has to be the latest one",
        MaestroBadRequestException.class,
        "Cannot STOP the workflow instance run [1] as it is not the latest run [2]",
        () -> actionHandler.stop("test-workflow", 1, 1, user));
  }

  @Test
  public void testInvalidExecutionId() {
    when(instance.getExecutionId()).thenReturn(null);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    AssertHelper.assertThrows(
        "execution_id cannot be null",
        NullPointerException.class,
        "execution_id cannot be null",
        () -> actionHandler.stop("test-workflow", 1, 1, user));
  }

  @Test
  public void testNoOpTerminate() {
    when(instance.getRunStatus()).thenReturn(WorkflowInstance.Status.STOPPED);
    when(instance.getIdentity()).thenReturn("test-id");
    assertTrue(actionHandler.stop("test-workflow", 1, 1, user).isCompleted());
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(0))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());
    verify(actionDao, times(0)).terminate(any(), any(), any(), anyString());

    when(instance.getRunStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(instance.getIdentity()).thenReturn("test-id");
    assertTrue(actionHandler.kill("test-workflow", 1, 1, user).isCompleted());
    verify(instanceDao, times(2)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(0))
        .tryTerminateQueuedInstance(any(), eq(WorkflowInstance.Status.FAILED), anyString());
    verify(actionDao, times(0)).terminate(any(), any(), any(), anyString());
  }

  @Test
  public void testInvalidStop() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.TIMED_OUT);
    when(instance.getIdentity()).thenReturn("test-id");
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [TIMED_OUT] to [STOPPED]",
        () -> actionHandler.stop("test-workflow", 1, 1, user));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.STOPPED);
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [STOPPED] to [FAILED]",
        () -> actionHandler.kill("test-workflow", 1, 1, user));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [FAILED] to [STOPPED]",
        () -> actionHandler.stop("test-workflow", 1, 1, user));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [SUCCEEDED] to [STOPPED]",
        () -> actionHandler.stop("test-workflow", 1, 1, user));
  }

  @Test
  public void testInvalidKill() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.TIMED_OUT);
    when(instance.getIdentity()).thenReturn("test-id");
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [TIMED_OUT] to [FAILED]",
        () -> actionHandler.kill("test-workflow", 1, 1, user));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.STOPPED);
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [STOPPED] to [FAILED]",
        () -> actionHandler.kill("test-workflow", 1, 1, user));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [FAILED] to [STOPPED]",
        () -> actionHandler.stop("test-workflow", 1, 1, user));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    AssertHelper.assertThrows(
        "fail to stop the workflow instance",
        MaestroInvalidStatusException.class,
        "Cannot terminate a workflow instance test-id from status [SUCCEEDED] to [FAILED]",
        () -> actionHandler.kill("test-workflow", 1, 1, user));
  }

  @Test
  public void testRestart() {
    WorkflowInstance wfInstance = new WorkflowInstance();
    wfInstance.setInitiator(new ManualInitiator());
    wfInstance.setStatus(WorkflowInstance.Status.SUCCEEDED);
    wfInstance.setWorkflowInstanceId(10L);
    wfInstance.setWorkflowRunId(1L);
    wfInstance.setWorkflowId("test-workflow");
    wfInstance.setRuntimeWorkflow(Workflow.builder().build());
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 10L, null).build())
            .build();

    when(instanceDao.getWorkflowInstance("test-workflow", 10L, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(wfInstance);
    when(runStrategyDao.startWithRunStrategy(any(), any())).thenReturn(1);
    when(instanceDao.getLatestWorkflowInstanceStatus(any(), anyLong()))
        .thenReturn(WorkflowInstance.Status.SUCCEEDED);

    RunResponse response = actionHandler.restart(request);
    assertEquals("test-workflow", response.getWorkflowId());
    assertEquals(10L, response.getWorkflowInstanceId());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
  }

  @Test
  public void testRestartNonTerminalError() {
    WorkflowInstance wfInstance = new WorkflowInstance();
    wfInstance.setInitiator(new ManualInitiator());
    wfInstance.setStatus(WorkflowInstance.Status.IN_PROGRESS);
    wfInstance.setWorkflowInstanceId(10L);
    wfInstance.setWorkflowRunId(1L);
    wfInstance.setWorkflowId("test-workflow");
    wfInstance.setRuntimeWorkflow(Workflow.builder().build());
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 1L, null).build())
            .build();
    when(instanceDao.getWorkflowInstance("test-workflow", 1L, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(wfInstance);

    AssertHelper.assertThrows(
        "Cannot restart a non-terminal state instance",
        MaestroBadRequestException.class,
        "workflow instance [test-workflow][1] does not support restart action as it is in a non-terminal status [IN_PROGRESS]",
        () -> actionHandler.restart(request));
  }

  @Test
  public void testRestartDelegated() {
    WorkflowInstance wfInstance = new WorkflowInstance();
    SubworkflowInitiator initiator = new SubworkflowInitiator();
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId("foo");
    info.setInstanceId(123L);
    info.setRunId(2L);
    info.setStepId("bar");
    initiator.setAncestors(Collections.singletonList(info));
    wfInstance.setInitiator(initiator);
    wfInstance.setStatus(WorkflowInstance.Status.FAILED);
    wfInstance.setWorkflowInstanceId(10L);
    wfInstance.setWorkflowRunId(1L);
    wfInstance.setWorkflowId("test-workflow");
    wfInstance.setRuntimeWorkflow(Workflow.builder().build());
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 1L, null).build())
            .build();

    WorkflowInstance parentInstance = new WorkflowInstance();
    parentInstance.setWorkflowId("foo");
    parentInstance.setWorkflowInstanceId(123L);
    parentInstance.setStatus(WorkflowInstance.Status.IN_PROGRESS);
    parentInstance.setAggregatedInfo(new WorkflowInstanceAggregatedInfo());
    parentInstance
        .getAggregatedInfo()
        .setStepAggregatedViews(
            Collections.singletonMap(
                "bar", StepAggregatedView.builder().status(StepInstance.Status.RUNNING).build()));

    when(instanceDao.getWorkflowInstance("test-workflow", 1L, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(wfInstance);
    when(instanceDao.getWorkflowInstance("foo", 123L, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(parentInstance);

    RunResponse response = actionHandler.restart(request);
    assertEquals("foo", response.getWorkflowId());
    assertEquals(123L, response.getWorkflowInstanceId());
    assertEquals(RunResponse.Status.DELEGATED, response.getStatus());

    parentInstance
        .getAggregatedInfo()
        .setStepAggregatedViews(
            Collections.singletonMap(
                "bar",
                StepAggregatedView.builder().status(StepInstance.Status.NOT_CREATED).build()));
    AssertHelper.assertThrows(
        "step id is not in the DAG",
        IllegalArgumentException.class,
        "step [bar] is not created in the DAG",
        () -> actionHandler.restart(request));
  }

  @Test
  public void testRestartSubworkflow() {
    WorkflowInstance wfInstance = new WorkflowInstance();
    SubworkflowInitiator initiator = new SubworkflowInitiator();
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId("foo");
    info.setInstanceId(123L);
    info.setRunId(2L);
    info.setStepId("bar");
    initiator.setAncestors(Collections.singletonList(info));
    wfInstance.setInitiator(initiator);
    wfInstance.setStatus(WorkflowInstance.Status.SUCCEEDED);
    wfInstance.setWorkflowInstanceId(10L);
    wfInstance.setWorkflowRunId(3L);
    wfInstance.setWorkflowId("test-workflow");
    wfInstance.setRuntimeWorkflow(Workflow.builder().build());

    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 10L, null).build())
            .build();
    when(runStrategyDao.startWithRunStrategy(any(), any())).thenReturn(1);
    when(instanceDao.getLatestWorkflowInstanceStatus(any(), anyLong()))
        .thenReturn(WorkflowInstance.Status.SUCCEEDED);
    doNothing().when(workflowHelper).updateWorkflowInstance(any(), any());

    RunResponse response = actionHandler.restartDirectly(wfInstance, request);
    assertEquals("test-workflow", response.getWorkflowId());
    assertEquals(10L, response.getWorkflowInstanceId());
    assertEquals(3L, response.getWorkflowRunId());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
    verify(workflowHelper, times(1)).updateWorkflowInstance(any(), any());
  }

  @Test
  public void testRestartForeach() {
    WorkflowInstance wfInstance = new WorkflowInstance();
    ForeachInitiator initiator = new ForeachInitiator();
    UpstreamInitiator.Info info = new UpstreamInitiator.Info();
    info.setWorkflowId("foo");
    info.setInstanceId(123L);
    info.setRunId(2L);
    info.setStepId("bar");
    initiator.setAncestors(Collections.singletonList(info));
    wfInstance.setInitiator(initiator);
    wfInstance.setStatus(WorkflowInstance.Status.SUCCEEDED);
    wfInstance.setWorkflowInstanceId(10L);
    wfInstance.setWorkflowRunId(3L);
    wfInstance.setWorkflowId("test-workflow");
    wfInstance.setRuntimeWorkflow(Workflow.builder().build());

    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 10L, null).build())
            .build();
    when(runStrategyDao.startWithRunStrategy(any(), any())).thenReturn(1);
    when(instanceDao.getLatestWorkflowInstanceStatus(any(), anyLong()))
        .thenReturn(WorkflowInstance.Status.SUCCEEDED);
    doNothing().when(workflowHelper).updateWorkflowInstance(any(), any());

    RunResponse response = actionHandler.restartDirectly(wfInstance, request);
    assertEquals("test-workflow", response.getWorkflowId());
    assertEquals(10L, response.getWorkflowInstanceId());
    assertEquals(3L, response.getWorkflowRunId());
    assertEquals(RunResponse.Status.WORKFLOW_RUN_CREATED, response.getStatus());
    verify(workflowHelper, times(1)).updateWorkflowInstance(any(), any());
  }

  @Test
  public void testInvalidRestart() {
    when(runStrategyDao.startWithRunStrategy(any(), any())).thenReturn(1);

    WorkflowInstance wfInstance = new WorkflowInstance();
    wfInstance.setInitiator(new ManualInitiator());
    wfInstance.setStatus(WorkflowInstance.Status.IN_PROGRESS);
    wfInstance.setWorkflowInstanceId(10L);
    wfInstance.setWorkflowRunId(1L);
    wfInstance.setWorkflowId("test-workflow");
    RunRequest request =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 1L, null).build())
            .build();
    when(instanceDao.getWorkflowInstance("test-workflow", 1L, Constants.LATEST_INSTANCE_RUN, true))
        .thenReturn(wfInstance);

    AssertHelper.assertThrows(
        "workflow instance does not support restart action",
        MaestroBadRequestException.class,
        "workflow instance [test-workflow][1] does not support restart action as it is in a non-terminal status [IN_PROGRESS]",
        () -> actionHandler.restart(request));

    wfInstance.setInitiator(new ManualInitiator());
    RunRequest request2 =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.START_FRESH_NEW_RUN)
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 1L, null).build())
            .build();
    AssertHelper.assertThrows(
        "Cannot restart a workflow instance for new instance",
        MaestroBadRequestException.class,
        "workflow instance [test-workflow][1] does not support restart action as it is in a non-terminal status [IN_PROGRESS]",
        () -> actionHandler.restart(request2));

    RunRequest request3 =
        RunRequest.builder()
            .initiator(new ManualInitiator())
            .currentPolicy(RunPolicy.RESTART_FROM_BEGINNING)
            .stepRunParams(Collections.singletonMap("job1", Collections.emptyMap()))
            .restartConfig(
                RestartConfig.builder().addRestartNode("test-workflow", 1L, null).build())
            .build();
    wfInstance.setStatus(WorkflowInstance.Status.FAILED);
    AssertHelper.assertThrows(
        "Cannot restart a workflow instance for new instance",
        IllegalArgumentException.class,
        "Cannot restart a workflow instance [test-workflow][10][1] as it does not match run request [test-workflow][1]",
        () -> actionHandler.restart(request3));
  }

  @Test
  public void testUnblockLatest() {
    when(instanceDao.tryUnblockFailedWorkflowInstance(any(), anyLong(), anyLong(), any()))
        .thenReturn(true);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(workflowDao.getRunStrategy("test-workflow"))
        .thenReturn(RunStrategy.create("STRICT_SEQUENTIAL"));
    boolean res = actionHandler.unblockLatest("test-workflow", 1, user).isCompleted();
    assertTrue(res);
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryUnblockFailedWorkflowInstance(eq("test-workflow"), eq(1L), eq(1L), any());
  }

  @Test
  public void testUnblock() {
    when(instanceDao.tryUnblockFailedWorkflowInstance(any(), anyLong(), anyLong(), any()))
        .thenReturn(true);
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(workflowDao.getRunStrategy("test-workflow"))
        .thenReturn(RunStrategy.create("STRICT_SEQUENTIAL"));
    var resp = actionHandler.unblock("test-workflow", 1, 1, user);
    assertTrue(resp.isCompleted());
    assertEquals("UNBLOCK", resp.getTimelineEvent().asAction().getAction());
    assertEquals("Unblocked the workflow instance.", resp.getTimelineEvent().getMessage());
    verify(instanceDao, times(1)).getLatestWorkflowInstanceRun("test-workflow", 1);
    verify(instanceDao, times(1))
        .tryUnblockFailedWorkflowInstance(eq("test-workflow"), eq(1L), eq(1L), any());
  }

  @Test
  public void testInvalidUnblock() {
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(workflowDao.getRunStrategy("test-workflow"))
        .thenReturn(RunStrategy.create("STRICT_SEQUENTIAL"));
    when(instance.getStatus()).thenReturn(WorkflowInstance.Status.STOPPED);
    AssertHelper.assertThrows(
        "can only unblock failed instance",
        MaestroInvalidStatusException.class,
        "from status [STOPPED] as it is not FAILED status",
        () -> actionHandler.unblock("test-workflow", 1, 1, user));
  }
}
