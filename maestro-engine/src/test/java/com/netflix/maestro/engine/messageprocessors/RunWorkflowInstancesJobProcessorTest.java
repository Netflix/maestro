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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.handlers.WorkflowRunner;
import com.netflix.maestro.engine.jobevents.RunWorkflowInstancesJobEvent;
import com.netflix.maestro.engine.processors.RunWorkflowInstancesJobProcessor;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.models.instance.WorkflowInstance;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class RunWorkflowInstancesJobProcessorTest extends MaestroEngineBaseTest {
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroFlowDao flowDao;
  @Mock private WorkflowRunner workflowRunner;
  @Mock private WorkflowInstance instance1;
  @Mock private WorkflowInstance instance2;
  @Mock private WorkflowInstance instance3;

  private final String workflowId = "sample-minimal-wf";
  private RunWorkflowInstancesJobProcessor jobProcessor;
  private RunWorkflowInstancesJobEvent jobEvent;

  @Before
  public void before() throws Exception {
    jobProcessor = new RunWorkflowInstancesJobProcessor(instanceDao, flowDao, workflowRunner);
    jobEvent = RunWorkflowInstancesJobEvent.init(workflowId);
    jobEvent.addOneRun(1L, 1L, "uuid1");
    jobEvent.addOneRun(2L, 1L, "uuid2");
    jobEvent.addOneRun(3L, 1L, "uuid3");
    when(instanceDao.getWorkflowInstanceRun(workflowId, 1L, 1L)).thenReturn(instance1);
    when(instanceDao.getWorkflowInstanceRun(workflowId, 2L, 1L)).thenReturn(instance2);
    when(instanceDao.getWorkflowInstanceRun(workflowId, 3L, 1L)).thenReturn(instance3);
    when(workflowRunner.start(instance1)).thenReturn("a");
    when(workflowRunner.start(instance2)).thenReturn("b");
    when(workflowRunner.start(instance3)).thenReturn("c");
    when(instance1.getGroupInfo()).thenReturn(1L);
    when(instance2.getGroupInfo()).thenReturn(2L);
    when(instance3.getGroupInfo()).thenReturn(3L);
    when(instance1.getIdentity()).thenReturn("[" + workflowId + "][1][1]");
    when(instance2.getIdentity()).thenReturn("[" + workflowId + "][2][1]");
    when(instance3.getIdentity()).thenReturn("[" + workflowId + "][3][1]");
  }

  @Test
  public void testLaunchWorkflowInstances() {
    when(flowDao.existFlowWithSameKeys(anyLong(), any())).thenReturn(false);
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.isFreshRun()).thenReturn(true);
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instance2.isFreshRun()).thenReturn(true);
    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    when(instance3.isFreshRun()).thenReturn(true);
    jobProcessor.process(() -> jobEvent);
    verify(workflowRunner, times(1)).start(instance1);
    verify(workflowRunner, times(1)).start(instance2);
    verify(workflowRunner, times(1)).start(instance3);
    verify(flowDao, times(3)).existFlowWithSameKeys(anyLong(), anyString());
    verify(flowDao, times(1)).existFlowWithSameKeys(0L, instance1.getWorkflowUuid());
    verify(flowDao, times(1)).existFlowWithSameKeys(1L, instance2.getWorkflowUuid());
    verify(flowDao, times(1)).existFlowWithSameKeys(1L, instance3.getWorkflowUuid());
    verify(workflowRunner, times(3)).start(any());
  }

  @Test
  public void testNotLaunchWorkflowInstances() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid-not-match");
    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    when(flowDao.existFlowWithSameKeys(anyLong(), any())).thenReturn(true);
    jobProcessor.process(() -> jobEvent);
    verify(instance1, times(0)).getWorkflowUuid();
    verify(flowDao, times(0)).existFlowWithSameKeys(0, "uuid1");
    verify(workflowRunner, times(0)).start(instance1);
    verify(instance2, times(2)).getWorkflowUuid();
    verify(flowDao, times(0)).existFlowWithSameKeys(1, "uuid2");
    verify(workflowRunner, times(0)).start(instance2);
    verify(instance3, times(2)).getWorkflowUuid();
    verify(flowDao, times(1)).existFlowWithSameKeys(1, "uuid3");
    verify(workflowRunner, times(0)).start(instance3);
    verify(flowDao, times(1)).existFlowWithSameKeys(anyLong(), anyString());
    verify(workflowRunner, times(0)).start(any());
  }

  @Test
  public void testLaunchWorkflowInstancesMaestroNotFoundException() {
    when(instance1.getStatus()).thenThrow(new MaestroNotFoundException("test"));
    when(instance2.getStatus()).thenThrow(new MaestroNotFoundException("test"));
    when(instance3.getStatus()).thenThrow(new MaestroNotFoundException("test"));
    jobProcessor.process(() -> jobEvent);
    verify(instance1, times(0)).getWorkflowUuid();
    verify(instance2, times(0)).getWorkflowUuid();
    verify(instance3, times(0)).getWorkflowUuid();
    verify(flowDao, times(0)).existFlowWithSameKeys(anyLong(), anyString());
    verify(workflowRunner, times(0)).start(any());
  }

  @Test
  public void testLaunchWorkflowInstancesNullPointerException() {
    when(instance1.getStatus()).thenThrow(new NullPointerException("test"));
    AssertHelper.assertThrows(
        "Something is null",
        MaestroInternalError.class,
        "Something is null",
        () -> jobProcessor.process(() -> jobEvent));
  }

  @Test
  public void testLaunchWorkflowInstancesRuntimeException() {
    when(instance1.getStatus()).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Failed to run",
        MaestroRetryableError.class,
        "Failed to run a workflow and will retry to run it.",
        () -> jobProcessor.process(() -> jobEvent));
  }

  @Test
  public void testRestartWorkflowInstances() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance1.getWorkflowUuid()).thenReturn("uuid1");
    when(instance1.isFreshRun()).thenReturn(true);
    when(flowDao.existFlowWithSameKeys(1, "uuid1")).thenReturn(false);
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid2");
    when(instance2.isFreshRun()).thenReturn(false);
    when(instance2.getWorkflowId()).thenReturn(workflowId);
    when(instance2.getWorkflowInstanceId()).thenReturn(2L);
    when(instance2.getWorkflowRunId()).thenReturn(2L);
    when(flowDao.existFlowWithSameKeys(2, "uuid2")).thenReturn(false);
    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    when(instance3.isFreshRun()).thenReturn(false);
    when(instance3.getWorkflowId()).thenReturn(workflowId);
    when(instance3.getWorkflowInstanceId()).thenReturn(3L);
    when(instance3.getWorkflowRunId()).thenReturn(2L);
    when(flowDao.existFlowWithSameKeys(3, "uuid3")).thenReturn(false);
    jobProcessor.process(() -> jobEvent);
    verify(workflowRunner, times(1)).start(instance1);
    verify(workflowRunner, times(1)).restart(instance2);
    verify(workflowRunner, times(1)).restart(instance3);
    verify(flowDao, times(3)).existFlowWithSameKeys(anyLong(), anyString());
  }
}
