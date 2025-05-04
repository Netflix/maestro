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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceActionDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Actions;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.queue.jobevents.TerminateThenRunJobEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class TerminateThenRunJobEventProcessorTest extends MaestroEngineBaseTest {
  @Mock private WorkflowRunner workflowRunner;
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceActionDao actionDao;
  @Mock private WorkflowInstance instance1;
  @Mock private WorkflowInstance instance2;
  @Mock private WorkflowInstance instance3;

  private final String workflowId = "sample-minimal-wf";
  private final User tester = User.create("tester");
  private TerminateThenRunJobEventProcessor processor;
  private TerminateThenRunJobEvent jobEvent1;
  private TerminateThenRunJobEvent jobEvent2;

  @Before
  public void before() throws Exception {
    processor = new TerminateThenRunJobEventProcessor(instanceDao, actionDao, workflowRunner);
    jobEvent1 =
        TerminateThenRunJobEvent.init(
            workflowId, Actions.WorkflowInstanceAction.STOP, tester, "test-reason");
    jobEvent1.addOneRun(1L, 1L, "uuid1");
    jobEvent1.addOneRun(2L, 1L, "uuid2");
    jobEvent1.addRunAfter(3L, 1L, "uuid3");
    jobEvent2 =
        TerminateThenRunJobEvent.init(
            workflowId, Actions.WorkflowInstanceAction.KILL, tester, "test-reason");
    jobEvent2.addOneRun(1L, 1L, "uuid1");
    jobEvent2.addOneRun(2L, 1L, "uuid2");
    jobEvent2.addOneRun(3L, 1L, "uuid3");
    when(instanceDao.getWorkflowInstanceRun(workflowId, 1L, 1L)).thenReturn(instance1);
    when(instanceDao.getWorkflowInstanceRun(workflowId, 2L, 1L)).thenReturn(instance2);
    when(instanceDao.getWorkflowInstanceRun(workflowId, 3L, 1L)).thenReturn(instance3);
    when(instance1.getWorkflowId()).thenReturn(workflowId);
    when(instance1.getWorkflowInstanceId()).thenReturn(1L);
    when(instance1.getWorkflowRunId()).thenReturn(1L);
    when(instance2.getWorkflowId()).thenReturn(workflowId);
    when(instance2.getWorkflowInstanceId()).thenReturn(2L);
    when(instance2.getWorkflowRunId()).thenReturn(1L);
    when(instance3.getWorkflowId()).thenReturn(workflowId);
    when(instance3.getWorkflowInstanceId()).thenReturn(3L);
    when(instance3.getWorkflowRunId()).thenReturn(1L);

    when(instance1.getGroupInfo()).thenReturn(1L);
    when(instance2.getGroupInfo()).thenReturn(2L);
    when(instance3.getGroupInfo()).thenReturn(3L);
    when(instance1.getIdentity()).thenReturn("[" + workflowId + "][1][1]");
    when(instance2.getIdentity()).thenReturn("[" + workflowId + "][2][1]");
    when(instance3.getIdentity()).thenReturn("[" + workflowId + "][3][1]");
  }

  @Test
  public void testTerminateWorkflowInstancesWithRunAfterNotDone() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.getExecutionId()).thenReturn("exe1");
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instance2.getExecutionId()).thenReturn("exe2");
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 1L, 1L)).thenReturn(null);
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 2L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    Assert.assertEquals(2, jobEvent1.getInstanceRunUuids().size());
    AssertHelper.assertThrows(
        "throw MaestroRetryableError to retry",
        MaestroRetryableError.class,
        "[InstanceRunUuid(instanceId=1, runId=1, uuid=uuid1)] is still terminating and will check it again",
        () -> processor.process(jobEvent1));

    Assert.assertTrue(jobEvent1.getInstanceRunUuids().isEmpty());
    Assert.assertEquals(2, jobEvent1.getTerminating().size());
    verify(actionDao, times(2)).terminate(any(), any(), any(), anyString());
    verify(actionDao, times(1))
        .terminate(instance1, tester, Actions.WorkflowInstanceAction.STOP, "test-reason");
    verify(workflowRunner, times(0)).run(any(WorkflowInstance.class), any());

    reset(actionDao);
    AssertHelper.assertThrows(
        "throw MaestroRetryableError to retry",
        MaestroRetryableError.class,
        "[InstanceRunUuid(instanceId=1, runId=1, uuid=uuid1)] is still terminating and will check it again",
        () -> processor.process(jobEvent1));
    verifyNoInteractions(actionDao);
    Assert.assertEquals(2, jobEvent1.getTerminating().size());
  }

  @Test
  public void testTerminateWorkflowInstancesWithRunAfterDone() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.getExecutionId()).thenReturn("exe1");
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instance2.getExecutionId()).thenReturn("exe2");
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 1L, 1L))
        .thenReturn(WorkflowInstance.Status.FAILED);
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 2L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    Assert.assertEquals(2, jobEvent1.getInstanceRunUuids().size());
    Assert.assertTrue(processor.process(jobEvent1).isEmpty());

    Assert.assertTrue(jobEvent1.getInstanceRunUuids().isEmpty());
    Assert.assertTrue(jobEvent1.getTerminating().isEmpty());
    verify(actionDao, times(2)).terminate(any(), any(), any(), anyString());
    verify(actionDao, times(1))
        .terminate(instance1, tester, Actions.WorkflowInstanceAction.STOP, "test-reason");
    verify(actionDao, times(1))
        .terminate(instance2, tester, Actions.WorkflowInstanceAction.STOP, "test-reason");
    verify(workflowRunner, times(1)).run(any(WorkflowInstance.class), any());
  }

  @Test
  public void testTerminateWorkflowInstancesWithoutRunAfterNotDone() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.getExecutionId()).thenReturn("exe1");
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instance2.getExecutionId()).thenReturn("exe2");
    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    when(instance3.getExecutionId()).thenReturn("exe3");
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 1L, 1L)).thenReturn(null);
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 2L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 3L, 1L))
        .thenReturn(WorkflowInstance.Status.CREATED);
    Assert.assertEquals(3, jobEvent2.getInstanceRunUuids().size());

    AssertHelper.assertThrows(
        "throw MaestroRetryableError to retry",
        MaestroRetryableError.class,
        "[InstanceRunUuid(instanceId=1, runId=1, uuid=uuid1)] is still terminating and will check it again",
        () -> processor.process(jobEvent2));

    Assert.assertTrue(jobEvent2.getInstanceRunUuids().isEmpty());
    Assert.assertEquals(3, jobEvent2.getTerminating().size());
    verify(actionDao, times(3)).terminate(any(), any(), any(), anyString());
    verify(actionDao, times(1))
        .terminate(instance1, tester, Actions.WorkflowInstanceAction.KILL, "test-reason");
    verify(actionDao, times(1))
        .terminate(instance2, tester, Actions.WorkflowInstanceAction.KILL, "test-reason");
    verify(actionDao, times(1))
        .terminate(instance3, tester, Actions.WorkflowInstanceAction.KILL, "test-reason");
    verify(workflowRunner, times(0)).run(any(WorkflowInstance.class), any());
  }

  @Test
  public void testTerminateWorkflowInstancesWithoutRunAfterDone() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instance2.getExecutionId()).thenReturn("exe2");
    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    when(instance3.getExecutionId()).thenReturn("exe3");
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 2L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 3L, 1L))
        .thenReturn(WorkflowInstance.Status.FAILED);
    Assert.assertEquals(3, jobEvent2.getInstanceRunUuids().size());
    Assert.assertTrue(processor.process(jobEvent2).isEmpty());

    Assert.assertTrue(jobEvent2.getInstanceRunUuids().isEmpty());
    Assert.assertTrue(jobEvent2.getTerminating().isEmpty());
    verify(actionDao, times(2)).terminate(any(), any(), any(), anyString());
    verify(actionDao, times(1))
        .terminate(instance2, tester, Actions.WorkflowInstanceAction.KILL, "test-reason");
    verify(actionDao, times(1))
        .terminate(instance3, tester, Actions.WorkflowInstanceAction.KILL, "test-reason");
    verify(workflowRunner, times(0)).run(any(WorkflowInstance.class), any());
  }

  @Test
  public void testNotTerminateWorkflowInstances() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.SUCCEEDED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.FAILED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 2L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 3L, 1L))
        .thenReturn(WorkflowInstance.Status.FAILED);
    Assert.assertTrue(processor.process(jobEvent1).isEmpty());
    verify(actionDao, times(0)).terminate(any(), any(), any(), anyString());
    verify(workflowRunner, times(1)).run(any(WorkflowInstance.class), any());
    reset(workflowRunner);

    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.STOPPED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    Assert.assertTrue(processor.process(jobEvent2).isEmpty());
    verify(actionDao, times(0)).terminate(any(), any(), any(), anyString());
    verify(workflowRunner, times(0)).run(any(WorkflowInstance.class), any());
  }

  @Test
  public void testTerminateWorkflowInstancesUuidNotMatchException() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getExecutionId()).thenReturn("exe1");
    when(instance1.getWorkflowUuid()).thenReturn("uuid2");
    AssertHelper.assertThrows(
        "Instance uuid mismatch",
        MaestroInternalError.class,
        "in job event does not match DB row uuid [uuid2]",
        () -> processor.process(jobEvent1));
  }

  @Test
  public void testTerminateWorkflowInstancesMaestroNotFoundException() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.getExecutionId()).thenReturn("exe1");
    when(instance2.getStatus()).thenThrow(new MaestroNotFoundException("test"));
    when(instanceDao.getWorkflowInstanceStatus(workflowId, 1L, 1L))
        .thenReturn(WorkflowInstance.Status.STOPPED);

    Assert.assertTrue(processor.process(jobEvent1).isEmpty());
    verify(actionDao, times(1)).terminate(any(), any(), any(), anyString());
    verify(actionDao, times(1))
        .terminate(instance1, tester, Actions.WorkflowInstanceAction.STOP, "test-reason");
    verify(workflowRunner, times(1)).run(any(WorkflowInstance.class), any());
  }

  @Test
  public void testTerminateWorkflowInstancesNullPointerException() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.getExecutionId()).thenReturn(null);
    AssertHelper.assertThrows(
        "Something is null",
        MaestroInternalError.class,
        "Something is null",
        () -> processor.process(jobEvent1));

    when(instance1.getStatus()).thenThrow(new NullPointerException("test"));
    AssertHelper.assertThrows(
        "Something is null",
        MaestroInternalError.class,
        "Something is null",
        () -> processor.process(jobEvent1));
  }

  @Test
  public void testTerminateWorkflowInstancesRuntimeException() {
    when(instance1.getStatus()).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Failed to run",
        MaestroRetryableError.class,
        "Failed to terminate a workflow and will retry to terminate it.",
        () -> processor.process(jobEvent1));
  }
}
