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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroNotFoundException;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.dao.MaestroFlowDao;
import com.netflix.maestro.flow.runtime.FlowOperation;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class WorkflowRunnerTest extends MaestroEngineBaseTest {

  @Mock private MaestroFlowDao flowDao;
  @Mock private FlowOperation flowOperation;
  @Mock private WorkflowTranslator translator;
  @Mock private WorkflowHelper workflowHelper;

  @Mock private WorkflowInstance instance1;
  @Mock private WorkflowInstance instance2;
  @Mock private WorkflowInstance instance3;

  private final String workflowId = "sample-minimal-wf";

  private WorkflowRunner workflowRunner;

  @Before
  public void before() {
    this.workflowRunner = new WorkflowRunner(flowDao, flowOperation, translator, workflowHelper);
    when(flowOperation.startFlow(anyLong(), any(), any(), any(), anyMap())).thenReturn("test-uuid");
    when(instance1.getGroupInfo()).thenReturn(1L);
    when(instance2.getGroupInfo()).thenReturn(2L);
    when(instance3.getGroupInfo()).thenReturn(3L);
    when(instance1.getWorkflowId()).thenReturn(workflowId);
    when(instance2.getWorkflowId()).thenReturn(workflowId);
    when(instance3.getWorkflowId()).thenReturn(workflowId);
    when(instance1.getWorkflowInstanceId()).thenReturn(1L);
    when(instance2.getWorkflowInstanceId()).thenReturn(2L);
    when(instance3.getWorkflowInstanceId()).thenReturn(3L);
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

    workflowRunner.run(instance1, "uuid1");
    workflowRunner.run(instance2, "uuid2");
    workflowRunner.run(instance3, "uuid3");

    verify(flowDao, times(3)).existFlowWithSameKeys(anyLong(), anyString());
    verify(flowDao, times(1)).existFlowWithSameKeys(0L, instance1.getWorkflowUuid());
    verify(flowDao, times(1)).existFlowWithSameKeys(0L, instance2.getWorkflowUuid());
    verify(flowDao, times(1)).existFlowWithSameKeys(2L, instance3.getWorkflowUuid());
    verify(flowOperation, times(1))
        .startFlow(eq(0L), eq("uuid1"), eq("[sample-minimal-wf][1]"), any(), any());
    verify(flowOperation, times(1))
        .startFlow(eq(0L), eq("uuid2"), eq("[sample-minimal-wf][2]"), any(), any());
    verify(flowOperation, times(1))
        .startFlow(eq(2L), eq("uuid3"), eq("[sample-minimal-wf][3]"), any(), any());
  }

  @Test
  public void testNotLaunchWorkflowInstances() {
    when(instance1.getStatus()).thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instance2.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance2.getWorkflowUuid()).thenReturn("uuid-not-match");
    when(instance3.getStatus()).thenReturn(WorkflowInstance.Status.CREATED);
    when(instance3.getWorkflowUuid()).thenReturn("uuid3");
    when(flowDao.existFlowWithSameKeys(anyLong(), any())).thenReturn(true);

    workflowRunner.run(instance1, "uuid1");
    workflowRunner.run(instance2, "uuid2");
    workflowRunner.run(instance3, "uuid3");

    verify(instance1, times(0)).getWorkflowUuid();
    verify(flowDao, times(0)).existFlowWithSameKeys(0, "uuid1");
    verify(instance2, times(2)).getWorkflowUuid();
    verify(flowDao, times(0)).existFlowWithSameKeys(0, "uuid2");
    verify(instance3, times(2)).getWorkflowUuid();
    verify(flowDao, times(1)).existFlowWithSameKeys(2, "uuid3");
    verify(flowDao, times(1)).existFlowWithSameKeys(anyLong(), anyString());
    verify(flowOperation, times(0)).startFlow(anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testLaunchWorkflowInstanceMaestroNotFoundException() {
    when(instance1.getStatus()).thenThrow(new MaestroNotFoundException("test"));
    workflowRunner.run(instance1, "uuid1");
    verify(instance1, times(0)).getWorkflowUuid();
    verify(flowDao, times(0)).existFlowWithSameKeys(anyLong(), anyString());
    verify(flowOperation, times(0)).startFlow(anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testLaunchWorkflowInstanceNullPointerException() {
    when(instance1.getStatus()).thenThrow(new NullPointerException("test"));
    AssertHelper.assertThrows(
        "Something is null",
        MaestroInternalError.class,
        "Something is null",
        () -> workflowRunner.run(instance1, "uuid1"));
  }

  @Test
  public void testLaunchWorkflowInstanceRuntimeException() {
    when(instance1.getStatus()).thenThrow(new RuntimeException("test"));
    AssertHelper.assertThrows(
        "Failed to run",
        MaestroRetryableError.class,
        "Failed to run a workflow and will retry to run it.",
        () -> workflowRunner.run(instance1, "uuid1"));
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

    workflowRunner.run(instance1, "uuid1");
    workflowRunner.run(instance2, "uuid2");
    workflowRunner.run(instance3, "uuid3");

    verify(flowDao, times(3)).existFlowWithSameKeys(anyLong(), anyString());
    verify(flowOperation, times(1))
        .startFlow(eq(0L), eq("uuid1"), eq("[sample-minimal-wf][1]"), any(), any());
    verify(flowOperation, times(1))
        .startFlow(eq(0L), eq("uuid2"), eq("[sample-minimal-wf][2]"), any(), any());
    verify(flowOperation, times(1))
        .startFlow(eq(2L), eq("uuid3"), eq("[sample-minimal-wf][3]"), any(), any());
  }

  @Test
  public void testStart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("test-workflow");
    instance.setGroupInfo(5L);
    instance.setWorkflowVersionId(1);
    instance.setRuntimeWorkflow(mock(Workflow.class));
    instance.setRuntimeDag(Collections.singletonMap("step1", new StepTransition()));
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        Collections.singletonMap(
            "stepid",
            Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    instance.setStepRunParams(stepRunParams);
    assertEquals("test-uuid", workflowRunner.start(instance));
    verify(translator, times(1)).translate(instance);
    verify(flowOperation, times(1)).startFlow(anyLong(), any(), any(), any(), anyMap());
  }

  @Test
  public void testRestart() {
    WorkflowInstance instance = new WorkflowInstance();
    instance.setWorkflowId("test-workflow");
    instance.setGroupInfo(5L);
    instance.setWorkflowVersionId(1);
    instance.setRuntimeWorkflow(mock(Workflow.class));
    instance.setRuntimeDag(Collections.singletonMap("step1", new StepTransition()));
    Map<String, Map<String, ParamDefinition>> stepRunParams =
        Collections.singletonMap(
            "stepid",
            Collections.singletonMap("p1", ParamDefinition.buildParamDefinition("p1", "d1")));
    instance.setStepRunParams(stepRunParams);
    assertEquals("test-uuid", workflowRunner.restart(instance));
    verify(translator, times(1)).translate(instance);
    verify(flowOperation, times(1)).startFlow(anyLong(), any(), any(), any(), anyMap());
  }
}
