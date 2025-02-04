/*
 * Copyright 2025 Netflix, Inc.
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.engine.transformation.StepTranslator;
import com.netflix.maestro.engine.transformation.WorkflowTranslator;
import com.netflix.maestro.engine.utils.RollupAggregationHelper;
import com.netflix.maestro.engine.utils.StepHelper;
import com.netflix.maestro.engine.utils.WorkflowHelper;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.SubworkflowStep;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.instance.StepDependencies;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class MaestroExecutionPreparerTest extends MaestroEngineBaseTest {
  @Mock MaestroWorkflowInstanceDao instanceDao;
  @Mock MaestroStepInstanceDao stepInstanceDao;
  @Mock WorkflowHelper workflowHelper;
  @Mock RollupAggregationHelper aggregationHelper;
  @Mock WorkflowSummary workflowSummary;
  private WorkflowTranslator translator;
  private MaestroExecutionPreparer executionPreparer;
  private Flow flow;
  private Task task;

  @Before
  public void setUp() {
    translator = new WorkflowTranslator(new StepTranslator());
    executionPreparer =
        new MaestroExecutionPreparer(
            instanceDao, stepInstanceDao, translator, workflowHelper, aggregationHelper, MAPPER);
    flow =
        new Flow(
            10,
            "test-flow-id",
            1,
            System.currentTimeMillis() + 3600000,
            "[sample-dag-test-3][1][1]");
    flow.setInput(Map.of(Constants.WORKFLOW_SUMMARY_FIELD, workflowSummary));
    flow.setFlowDef(new FlowDef());
    flow.getFlowDef().setTasks(List.of(List.of(new TaskDef("task1", "noop", null, null))));
    task = flow.newTask(new TaskDef("task1", "noop", null, null), true);
  }

  @Test
  public void testAddExtraTasksAndInputForStepDependencies() {
    when(workflowSummary.isFreshRun()).thenReturn(false);
    when(workflowSummary.getRunPolicy()).thenReturn(RunPolicy.RESTART_FROM_BEGINNING);
    var stepDependencies =
        Map.of(
            "step1",
            Map.of(
                StepDependencyType.SIGNAL,
                new StepDependencies(StepDependencyType.SIGNAL, List.of())));
    when(stepInstanceDao.getAllStepDependencies(any(), anyLong(), anyLong()))
        .thenReturn(stepDependencies);

    executionPreparer.addExtraTasksAndInput(flow, task);
    assertEquals(
        Map.of(Constants.ALL_STEP_DEPENDENCIES_FIELD, stepDependencies), task.getOutputData());
    verify(stepInstanceDao, times(1)).getAllStepDependencies(any(), anyLong(), anyLong());
    verify(stepInstanceDao, times(0)).getAllLatestStepFromAncestors(any(), anyLong(), any());
    assertTrue(flow.getFinishedTasks().isEmpty());
  }

  @Test
  public void testAddExtraTasksAndInputWithFreshRun() {
    when(workflowSummary.isFreshRun()).thenReturn(true);

    executionPreparer.addExtraTasksAndInput(flow, task);
    assertTrue(task.getOutputData().isEmpty());
    verify(stepInstanceDao, times(0)).getAllStepDependencies(any(), anyLong(), anyLong());
    verify(stepInstanceDao, times(0)).getAllLatestStepFromAncestors(any(), anyLong(), any());
    assertTrue(flow.getFinishedTasks().isEmpty());
  }

  @Test
  public void testAddExtraTasksAndInputWithDummyTasks() {
    when(workflowSummary.isFreshRun()).thenReturn(false);
    when(workflowSummary.getRunPolicy()).thenReturn(RunPolicy.RESTART_FROM_INCOMPLETE);
    when(stepInstanceDao.getAllStepDependencies(any(), anyLong(), anyLong())).thenReturn(null);

    Step stepDef = new SubworkflowStep();
    when(workflowSummary.getStepMap()).thenReturn(new HashMap<>(Map.of("step1", stepDef)));
    StepInstance stepInstance = new StepInstance();
    stepInstance.setStepId("step1");
    stepInstance.setDefinition(stepDef);
    stepInstance.setRuntimeState(new StepRuntimeState());
    stepInstance.setParams(Map.of("foo", buildParam("foo", "bar")));
    when(stepInstanceDao.getAllLatestStepFromAncestors(any(), anyLong(), any()))
        .thenReturn(Map.of("step1", stepInstance));

    executionPreparer.addExtraTasksAndInput(flow, task);
    assertTrue(task.getOutputData().isEmpty());
    verify(stepInstanceDao, times(1)).getAllStepDependencies(any(), anyLong(), anyLong());
    verify(stepInstanceDao, times(1))
        .getAllLatestStepFromAncestors(any(), anyLong(), eq(Set.of("step1")));
    assertEquals(1, flow.getFinishedTasks().size());
    assertEquals("step1", flow.getFinishedTasks().stream().findFirst().get().referenceTaskName());
    assertEquals(
        Task.Status.SKIPPED, flow.getFinishedTasks().stream().findFirst().get().getStatus());
    assertFalse(flow.getFinishedTasks().stream().findFirst().get().isActive());
    assertEquals(-1, flow.getFinishedTasks().stream().findFirst().get().getSeq());
    assertEquals(
        Set.of("foo"),
        StepHelper.retrieveRuntimeSummary(
                null, flow.getFinishedTasks().stream().findFirst().get().getOutputData())
            .getParams()
            .keySet());
  }

  @Test
  public void testCloneTask() {
    var clonedTask = executionPreparer.cloneTask(task);
    assertEquals(clonedTask, task);
    assertSame(clonedTask.getTaskDef(), task.getTaskDef());
    assertNotSame(clonedTask.getOutputData(), task.getOutputData());
  }

  @Test
  public void testResumeRunningWorkflow() throws Exception {
    var instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setStatus(WorkflowInstance.Status.IN_PROGRESS);
    var stepInstance =
        loadObject("fixtures/instances/sample-step-instance-running.json", StepInstance.class);
    when(instanceDao.getWorkflowInstanceRun("sample-dag-test-3", 1, 1)).thenReturn(instance);
    when(workflowHelper.createWorkflowSummaryFromInstance(instance)).thenCallRealMethod();
    when(stepInstanceDao.getStepInstanceViews("sample-dag-test-3", 1, 1))
        .thenReturn(List.of(stepInstance));

    assertFalse(executionPreparer.resume(flow));
    verify(instanceDao, times(1)).getWorkflowInstanceRun(any(), anyLong(), anyLong());
    verify(workflowHelper, times(1)).createWorkflowSummaryFromInstance(any());
    verify(stepInstanceDao, times(1)).getStepInstanceViews(any(), anyLong(), anyLong());
    assertEquals(Flow.Status.RUNNING, flow.getStatus());
    assertEquals(Constants.DEFAULT_START_TASK_NAME, flow.getPrepareTask().referenceTaskName());
    assertEquals(Constants.DEFAULT_END_TASK_NAME, flow.getMonitorTask().referenceTaskName());
    assertEquals(123456, flow.getFlowDef().getTimeoutInMillis());
    assertTrue(flow.getFlowDef().isFinalFlowStatusCallbackEnabled());
    assertEquals(
        Set.of("job1,job.2,#job4,job4", "#job3,job3"),
        flow.getFlowDef().getTasks().stream()
            .map(
                tasks ->
                    tasks.stream().map(TaskDef::taskReferenceName).collect(Collectors.joining(",")))
            .collect(Collectors.toSet()));
    assertEquals(3, flow.getSeq());
    assertEquals(instance.getStartTime(), flow.getPrepareTask().getStartTime());
    assertEquals(Set.of("job1"), flow.getRunningTasks().keySet());
  }

  @Test
  public void testResumeFailedWorkflow() throws Exception {
    var instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setStatus(WorkflowInstance.Status.FAILED);
    when(instanceDao.getWorkflowInstanceRun("sample-dag-test-3", 1, 1)).thenReturn(instance);
    when(workflowHelper.createWorkflowSummaryFromInstance(instance)).thenCallRealMethod();

    assertFalse(executionPreparer.resume(flow));
    verify(instanceDao, times(1)).getWorkflowInstanceRun(any(), anyLong(), anyLong());
    verify(workflowHelper, times(1)).createWorkflowSummaryFromInstance(any());
    verify(stepInstanceDao, times(0)).getStepInstanceViews(any(), anyLong(), anyLong());
    assertEquals(Flow.Status.COMPLETED, flow.getStatus());
    assertEquals(Constants.DEFAULT_START_TASK_NAME, flow.getPrepareTask().referenceTaskName());
    assertEquals(Constants.DEFAULT_END_TASK_NAME, flow.getMonitorTask().referenceTaskName());
    assertEquals(123456, flow.getFlowDef().getTimeoutInMillis());
    assertTrue(flow.getFlowDef().isFinalFlowStatusCallbackEnabled());
    assertEquals(
        Set.of("job1,job.2,#job4,job4", "#job3,job3"),
        flow.getFlowDef().getTasks().stream()
            .map(
                tasks ->
                    tasks.stream().map(TaskDef::taskReferenceName).collect(Collectors.joining(",")))
            .collect(Collectors.toSet()));
    assertEquals(3, flow.getSeq());
    assertNull(flow.getPrepareTask().getStartTime());
    assertTrue(flow.getRunningTasks().isEmpty());
  }

  @Test
  public void testResumeCreatedWorkflow() throws Exception {
    var instance =
        loadObject(
            "fixtures/instances/sample-workflow-instance-created.json", WorkflowInstance.class);
    instance.setExecutionId(null);
    when(instanceDao.getWorkflowInstanceRun("sample-dag-test-3", 1, 1)).thenReturn(instance);
    when(workflowHelper.createWorkflowSummaryFromInstance(instance)).thenCallRealMethod();

    assertTrue(executionPreparer.resume(flow));
    verify(instanceDao, times(1)).getWorkflowInstanceRun(any(), anyLong(), anyLong());
    verify(workflowHelper, times(1)).createWorkflowSummaryFromInstance(any());
    verify(stepInstanceDao, times(0)).getStepInstanceViews(any(), anyLong(), anyLong());
    assertEquals(Flow.Status.RUNNING, flow.getStatus());
    assertEquals(Constants.DEFAULT_START_TASK_NAME, flow.getPrepareTask().referenceTaskName());
    assertEquals(Constants.DEFAULT_END_TASK_NAME, flow.getMonitorTask().referenceTaskName());
    assertEquals(123456, flow.getFlowDef().getTimeoutInMillis());
    assertTrue(flow.getFlowDef().isFinalFlowStatusCallbackEnabled());
    assertEquals(
        Set.of("job1,job.2,#job4,job4", "#job3,job3"),
        flow.getFlowDef().getTasks().stream()
            .map(
                tasks ->
                    tasks.stream().map(TaskDef::taskReferenceName).collect(Collectors.joining(",")))
            .collect(Collectors.toSet()));
    assertEquals(3, flow.getSeq());
    assertNull(flow.getPrepareTask().getStartTime());
    assertTrue(flow.getRunningTasks().isEmpty());
  }
}
