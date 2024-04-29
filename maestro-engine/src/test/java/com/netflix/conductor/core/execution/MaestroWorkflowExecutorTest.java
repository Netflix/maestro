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
package com.netflix.conductor.core.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.metadata.MetadataMapperService;
import com.netflix.conductor.core.orchestration.ExecutionDAOFacade;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.service.ExecutionLockService;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeCallbackDelayPolicy;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.properties.MaestroConductorProperties;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import lombok.Setter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;

public class MaestroWorkflowExecutorTest extends MaestroEngineBaseTest {
  @Mock DeciderService deciderService;
  @Mock MetadataDAO metadataDAO;
  @Mock QueueDAO queueDAO;
  @Mock MetadataMapperService metadataMapperService;
  @Mock WorkflowStatusListener workflowStatusListener;
  @Mock ExecutionDAOFacade executionDAOFacade;
  @Mock MaestroConductorProperties props;
  @Mock ExecutionLockService executionLockService;
  @Mock ParametersUtils parametersUtils;
  @Mock StepRuntimeCallbackDelayPolicy callbackPolicy;

  private MaestroWorkflowExecutor maestroWorkflowExecutor;
  private Workflow workflow;
  private TestTask task1;
  private TestTask task2;

  @Before
  public void before() {
    this.maestroWorkflowExecutor =
        new MaestroWorkflowExecutor(
            deciderService,
            metadataDAO,
            queueDAO,
            metadataMapperService,
            workflowStatusListener,
            executionDAOFacade,
            props,
            executionLockService,
            parametersUtils,
            callbackPolicy,
            new MaestroWorkflowTaskRunner(),
            MAPPER);

    workflow = new Workflow();
    workflow.setWorkflowId("testRetryWorkflowId");
    workflow.setStatus(Workflow.WorkflowStatus.FAILED);
    WorkflowDef def = new WorkflowDef();
    def.setWorkflowStatusListenerEnabled(true);
    workflow.setWorkflowDefinition(def);

    // register maestro task type.
    task1 = new TestTask(Constants.DEFAULT_START_STEP_NAME);
    task1.setShouldThrow(false);
    task2 = new TestTask(Constants.MAESTRO_TASK_NAME);
    task2.setShouldThrow(false);
    new TestTask(SystemTaskType.JOIN.name());
    new TestTask(SystemTaskType.EXCLUSIVE_JOIN.name());
  }

  private static class TestTask extends WorkflowSystemTask {
    @Setter private boolean shouldThrow = false;

    public TestTask(String name) {
      super(name);
    }

    @Override
    public boolean execute(Workflow workflow, Task task, WorkflowExecutor executor) {
      if (shouldThrow) {
        throw new IllegalArgumentException("unexpected-exception");
      }
      return true;
    }

    @Override
    public void cancel(Workflow workflow, Task task, WorkflowExecutor executor) {
      if (shouldThrow) {
        throw new MaestroRetryableError("test-exception");
      }
    }
  }

  @Test
  public void testConfigureCallBack() {
    Task task = new Task();
    // stepRuntimeSummary object
    task.getOutputData()
        .put(Constants.STEP_RUNTIME_SUMMARY_FIELD, StepRuntimeSummary.builder().build());
    when(callbackPolicy.getCallBackDelayInSecs(any())).thenReturn(30L);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(1)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());
    // run time summary not present
    task.getOutputData().clear();
    task = new Task();
    when(callbackPolicy.getCallBackDelayInSecs(any())).thenReturn(30L);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    assertEquals(0, task.getCallbackAfterSeconds());
    verify(callbackPolicy, times(1)).getCallBackDelayInSecs(any());
    // null taskResult
    maestroWorkflowExecutor.configureCallbackInterval(null);
    verify(callbackPolicy, times(1)).getCallBackDelayInSecs(any());
    // map object as runtime summary
    task.getOutputData().clear();
    task.getOutputData().put(Constants.STEP_RUNTIME_SUMMARY_FIELD, new HashMap<>());
    when(callbackPolicy.getCallBackDelayInSecs(any())).thenReturn(30L);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(2)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());
  }

  @Test
  public void testConfigureCallBackForRunningForeachStepWithFirstPolling() {
    // running foreach step case with first polling with 0-delay callback
    Task task = new Task();
    task.setPollCount(Constants.FIRST_POLLING_COUNT_LIMIT);
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.RUNNING);
    state.setCreateTime(System.currentTimeMillis());
    task.getOutputData()
        .put(
            Constants.STEP_RUNTIME_SUMMARY_FIELD,
            StepRuntimeSummary.builder().runtimeState(state).type(StepType.FOREACH).build());
    when(callbackPolicy.getCallBackDelayInSecs(any())).thenReturn(30L);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(1)).getCallBackDelayInSecs(any());
    assertEquals(0L, task.getCallbackAfterSeconds());

    // if it happens too late
    state.setCreateTime(
        System.currentTimeMillis() - 2 * Constants.FIRST_POLL_TIME_BUFFER_IN_MILLIS);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(2)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());

    // if it polled too many times
    state.setCreateTime(System.currentTimeMillis());
    task.setPollCount(Constants.FIRST_POLLING_COUNT_LIMIT + 1);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(3)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());
  }

  @Test
  public void testConfigureCallBackForJustCreatedMaestroTaskWithFirstPolling() {
    // just created maestro task with first polling will have 0-delay callback
    Task task = new Task();
    task.setPollCount(Constants.FIRST_POLLING_COUNT_LIMIT);
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.CREATED);
    state.setCreateTime(System.currentTimeMillis());
    task.getOutputData()
        .put(
            Constants.STEP_RUNTIME_SUMMARY_FIELD,
            StepRuntimeSummary.builder().runtimeState(state).type(StepType.NOOP).build());
    when(callbackPolicy.getCallBackDelayInSecs(any())).thenReturn(30L);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(1)).getCallBackDelayInSecs(any());
    assertEquals(0L, task.getCallbackAfterSeconds());

    // if it happens too late
    state.setCreateTime(
        System.currentTimeMillis() - 2 * Constants.FIRST_POLL_TIME_BUFFER_IN_MILLIS);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(2)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());

    // if it polled too many times
    state.setCreateTime(System.currentTimeMillis());
    task.setPollCount(Constants.FIRST_POLLING_COUNT_LIMIT + 1);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(3)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());
  }

  @Test
  public void testConfigureCallBackForStartTaskWithFirstPolling() {
    // Start task with first polling will have 0-delay callback
    Task task = new Task();
    task.setReferenceTaskName(Constants.DEFAULT_START_STEP_NAME);
    task.setCallbackAfterSeconds(30L);
    task.setPollCount(Constants.FIRST_POLLING_COUNT_LIMIT);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(0)).getCallBackDelayInSecs(any());
    assertEquals(0L, task.getCallbackAfterSeconds());

    // if it is not start task
    task.setCallbackAfterSeconds(30L);
    task.setReferenceTaskName(Constants.DEFAULT_END_STEP_NAME);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(0)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());

    // if it polled too many times
    task.setPollCount(Constants.FIRST_POLLING_COUNT_LIMIT + 1);
    maestroWorkflowExecutor.configureCallbackInterval(task);
    verify(callbackPolicy, times(0)).getCallBackDelayInSecs(any());
    assertEquals(30L, task.getCallbackAfterSeconds());
  }

  @Test
  public void testCancelNonTerminalTasks() {
    Task startTask = new Task();
    startTask.setTaskId(UUID.randomUUID().toString());
    startTask.setTaskType(Constants.DEFAULT_START_STEP_NAME);
    startTask.setStatus(Task.Status.IN_PROGRESS);

    Task maestroTask = new Task();
    maestroTask.setTaskId(UUID.randomUUID().toString());
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setStatus(Task.Status.SCHEDULED);

    Task gateTask = new Task();
    gateTask.setTaskId(UUID.randomUUID().toString());
    gateTask.setTaskType(SystemTaskType.EXCLUSIVE_JOIN.name());
    gateTask.setStatus(Task.Status.COMPLETED);

    Task endTask = new Task();
    endTask.setTaskId(UUID.randomUUID().toString());
    endTask.setTaskType(SystemTaskType.JOIN.name());
    endTask.setStatus(Task.Status.IN_PROGRESS);

    workflow.getTasks().addAll(Arrays.asList(startTask, maestroTask, gateTask, endTask));

    List<String> erroredTasks = maestroWorkflowExecutor.cancelNonTerminalTasks(workflow);
    assertTrue(erroredTasks.isEmpty());
    ArgumentCaptor<Task> argumentCaptor = ArgumentCaptor.forClass(Task.class);
    verify(executionDAOFacade, times(3)).updateTask(argumentCaptor.capture());
    assertEquals(3, argumentCaptor.getAllValues().size());
    assertEquals(
        Constants.DEFAULT_START_STEP_NAME, argumentCaptor.getAllValues().get(0).getTaskType());
    assertEquals(Task.Status.CANCELED, argumentCaptor.getAllValues().get(0).getStatus());
    assertEquals(Constants.MAESTRO_TASK_NAME, argumentCaptor.getAllValues().get(1).getTaskType());
    assertEquals(Task.Status.CANCELED, argumentCaptor.getAllValues().get(1).getStatus());
    assertEquals(SystemTaskType.JOIN.name(), argumentCaptor.getAllValues().get(2).getTaskType());
    assertEquals(Task.Status.CANCELED, argumentCaptor.getAllValues().get(2).getStatus());
    verify(workflowStatusListener, times(1)).onWorkflowFinalizedIfEnabled(any(Workflow.class));
    verify(queueDAO, times(1)).remove(any(), any());
  }

  @Test
  public void testCancelNonTerminalTasksFailed() {
    Task mockTask1 = mock(Task.class);
    when(mockTask1.getTaskId()).thenReturn("task-id-1");
    when(mockTask1.getTaskType()).thenReturn(Constants.MAESTRO_PREFIX);
    when(mockTask1.getStatus()).thenReturn(Task.Status.IN_PROGRESS);

    workflow.getTasks().add(mockTask1);

    AssertHelper.assertThrows(
        "All tasks should have a known maestro task type.",
        IllegalArgumentException.class,
        "Invalid task type",
        () -> maestroWorkflowExecutor.cancelNonTerminalTasks(workflow));

    when(mockTask1.getTaskType()).thenReturn(Constants.DEFAULT_START_STEP_NAME);
    task1.setShouldThrow(true);
    Task maestroTask = new Task();
    maestroTask.setTaskId(UUID.randomUUID().toString());
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setStatus(Task.Status.SCHEDULED);

    workflow.getTasks().add(maestroTask);

    AssertHelper.assertThrows(
        "Cancel throws an exception will fail cancelNonTerminalTasks call.",
        MaestroRetryableError.class,
        "Error canceling tasks ",
        () -> maestroWorkflowExecutor.cancelNonTerminalTasks(workflow));
    ArgumentCaptor<Task> argumentCaptor = ArgumentCaptor.forClass(Task.class);
    verify(executionDAOFacade, times(1)).updateTask(argumentCaptor.capture());
    assertEquals(1, argumentCaptor.getAllValues().size());
    assertEquals(Constants.MAESTRO_TASK_NAME, argumentCaptor.getAllValues().get(0).getTaskType());
    assertEquals(Task.Status.CANCELED, argumentCaptor.getAllValues().get(0).getStatus());
    verify(queueDAO, times(0)).remove(any(), any());
  }

  @Test
  public void testFinalizedFailure() {
    Task startTask = new Task();
    startTask.setTaskId(UUID.randomUUID().toString());
    startTask.setTaskType(Constants.DEFAULT_START_STEP_NAME);
    startTask.setStatus(Task.Status.IN_PROGRESS);

    Task maestroTask = new Task();
    maestroTask.setTaskId(UUID.randomUUID().toString());
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setStatus(Task.Status.SCHEDULED);

    workflow.getTasks().addAll(Arrays.asList(startTask, maestroTask));

    doThrow(new MaestroRetryableError("test-finalize"))
        .when(workflowStatusListener)
        .onWorkflowFinalizedIfEnabled(any(Workflow.class));
    AssertHelper.assertThrows(
        "Cancel throws an exception will fail cancelNonTerminalTasks call.",
        MaestroRetryableError.class,
        "test-finalize",
        () -> maestroWorkflowExecutor.cancelNonTerminalTasks(workflow));

    ArgumentCaptor<Task> argumentCaptor2 = ArgumentCaptor.forClass(Task.class);
    verify(executionDAOFacade, times(2)).updateTask(argumentCaptor2.capture());
    assertEquals(2, argumentCaptor2.getAllValues().size());
    assertEquals(
        Constants.DEFAULT_START_STEP_NAME, argumentCaptor2.getAllValues().get(0).getTaskType());
    assertEquals(Task.Status.CANCELED, argumentCaptor2.getAllValues().get(0).getStatus());
    assertEquals(Constants.MAESTRO_TASK_NAME, argumentCaptor2.getAllValues().get(1).getTaskType());
    assertEquals(Task.Status.CANCELED, argumentCaptor2.getAllValues().get(1).getStatus());
    verify(workflowStatusListener, times(1)).onWorkflowFinalizedIfEnabled(any(Workflow.class));
    verify(queueDAO, times(0)).remove(any(), any());
  }

  @Test
  public void testExecuteSystemTaskWithoutUpdatingPollingCount() {
    String workflowId = "workflow-id";
    String taskId = "task-id-1";

    Task maestroTask = new Task();
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setReferenceTaskName("maestroTask");
    maestroTask.setWorkflowInstanceId(workflowId);
    maestroTask.setScheduledTime(System.currentTimeMillis());
    maestroTask.setTaskId(taskId);
    maestroTask.setStatus(Task.Status.IN_PROGRESS);
    maestroTask.setStartTime(123456L);

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

    when(executionDAOFacade.getTaskById(anyString())).thenReturn(maestroTask);
    when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

    maestroWorkflowExecutor.executeSystemTask(task2, taskId, 30);

    assertEquals(Task.Status.IN_PROGRESS, maestroTask.getStatus());
    assertEquals(1, maestroTask.getPollCount());
    verify(executionDAOFacade, times(0)).updateTask(any());
    assertEquals(123456, maestroTask.getStartTime());
  }

  @Test
  public void testExecuteSystemTaskPersistStartTime() {
    String workflowId = "workflow-id";
    String taskId = "task-id-1";

    Task maestroTask = new Task();
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setReferenceTaskName("maestroTask");
    maestroTask.setWorkflowInstanceId(workflowId);
    maestroTask.setScheduledTime(System.currentTimeMillis());
    maestroTask.setTaskId(taskId);
    maestroTask.setStatus(Task.Status.IN_PROGRESS);
    maestroTask.setStartTime(0);
    maestroTask.setCallbackAfterSeconds(0);

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

    when(executionDAOFacade.getTaskById(anyString())).thenReturn(maestroTask);
    when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

    maestroWorkflowExecutor.executeSystemTask(task2, taskId, 30);

    assertEquals(Task.Status.IN_PROGRESS, maestroTask.getStatus());
    assertEquals(1, maestroTask.getPollCount());
    verify(executionDAOFacade, times(1)).updateTask(any());
    assertTrue(maestroTask.getStartTime() > 0);
    assertEquals(30, maestroTask.getCallbackAfterSeconds());
  }

  @Test
  public void testExecuteSystemTaskThrowUnexpectedException() {
    String workflowId = "workflow-id";
    String taskId = "task-id-1";

    Task maestroTask = new Task();
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setReferenceTaskName("maestroTask");
    maestroTask.setWorkflowInstanceId(workflowId);
    maestroTask.setScheduledTime(System.currentTimeMillis());
    maestroTask.setTaskId(taskId);
    maestroTask.setStatus(Task.Status.IN_PROGRESS);
    maestroTask.setStartTime(123);
    maestroTask.setCallbackAfterSeconds(0);

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

    when(executionDAOFacade.getTaskById(anyString())).thenReturn(maestroTask);
    when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

    task2.setShouldThrow(true);
    maestroWorkflowExecutor.executeSystemTask(task2, taskId, 30);

    assertEquals(Task.Status.IN_PROGRESS, maestroTask.getStatus());
    assertEquals(1, maestroTask.getPollCount());
    verify(executionDAOFacade, times(0)).updateTask(any());
    assertEquals(0, maestroTask.getCallbackAfterSeconds());
  }

  @Test
  public void testResetOffset() {
    String workflowId = "workflow-id";
    String taskId = "task-id-1";
    String taskId1 = "task-id-2";

    Task maestroTask = new Task();
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setReferenceTaskName("maestroTask");
    maestroTask.setWorkflowInstanceId(workflowId);
    maestroTask.setScheduledTime(System.currentTimeMillis());
    maestroTask.setTaskId(taskId);
    maestroTask.setStatus(Task.Status.FAILED);
    maestroTask.setStartTime(123);
    maestroTask.setCallbackAfterSeconds(0);

    Task maestroTask1 = new Task();
    maestroTask1.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask1.setReferenceTaskName("maestroTask");
    maestroTask1.setWorkflowInstanceId(workflowId);
    maestroTask1.setScheduledTime(System.currentTimeMillis());
    maestroTask1.setTaskId(taskId1);
    maestroTask1.setStatus(Task.Status.SCHEDULED);
    maestroTask1.setStartTime(123);
    maestroTask1.setCallbackAfterSeconds(0);
    maestroTask1.setRetryCount(1);
    maestroTask1.setRetriedTaskId(taskId);

    List<Task> tasks = new ArrayList<>();
    tasks.add(maestroTask);
    tasks.add(maestroTask1);

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    workflow.setTasks(tasks);

    when(executionDAOFacade.getTaskById(taskId)).thenReturn(maestroTask);
    when(executionDAOFacade.getWorkflowById(workflowId, true)).thenReturn(workflow);
    when(queueDAO.resetOffsetTime(Constants.MAESTRO_TASK_NAME, taskId1)).thenReturn(true);

    assertTrue(maestroWorkflowExecutor.resetTaskOffset(taskId));
  }

  @Test
  public void testResetOffsetNotFound() {
    String workflowId = "workflow-id";
    String taskId = "task-id-1";
    String taskId1 = "task-id-2";

    Task maestroTask = new Task();
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setReferenceTaskName("maestroTask");
    maestroTask.setWorkflowInstanceId(workflowId);
    maestroTask.setScheduledTime(System.currentTimeMillis());
    maestroTask.setTaskId(taskId);
    maestroTask.setStatus(Task.Status.FAILED);
    maestroTask.setStartTime(123);
    maestroTask.setCallbackAfterSeconds(0);

    Task maestroTask1 = new Task();
    maestroTask1.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask1.setReferenceTaskName("maestroTask");
    maestroTask1.setWorkflowInstanceId(workflowId);
    maestroTask1.setScheduledTime(System.currentTimeMillis());
    maestroTask1.setTaskId(taskId1);
    maestroTask1.setStatus(Task.Status.SCHEDULED);
    maestroTask1.setStartTime(123);
    maestroTask1.setCallbackAfterSeconds(0);
    maestroTask1.setRetryCount(1);

    List<Task> tasks = new ArrayList<>();
    tasks.add(maestroTask);
    tasks.add(maestroTask1);

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    workflow.setTasks(tasks);

    when(executionDAOFacade.getTaskById(taskId)).thenReturn(maestroTask);
    when(executionDAOFacade.getWorkflowById(workflowId, true)).thenReturn(workflow);

    assertFalse(maestroWorkflowExecutor.resetTaskOffset(taskId));
    Mockito.verifyNoInteractions(queueDAO);
  }

  @Test
  public void testExecuteSystemTaskThrowExceptionDuringConfigureCallbackInterval() {
    String workflowId = "workflow-id";
    String taskId = "task-id-1";

    Task maestroTask = new Task();
    maestroTask.setTaskType(Constants.MAESTRO_TASK_NAME);
    maestroTask.setReferenceTaskName("maestroTask");
    maestroTask.setWorkflowInstanceId(workflowId);
    maestroTask.setScheduledTime(System.currentTimeMillis());
    maestroTask.setTaskId(taskId);
    maestroTask.setStatus(Task.Status.IN_PROGRESS);
    maestroTask.setStartTime(123);
    maestroTask.setCallbackAfterSeconds(0);
    maestroTask.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, null));

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

    when(executionDAOFacade.getTaskById(anyString())).thenReturn(maestroTask);
    when(executionDAOFacade.getWorkflowById(anyString(), anyBoolean())).thenReturn(workflow);

    task2.setShouldThrow(false);
    maestroWorkflowExecutor.executeSystemTask(task2, taskId, 30);

    assertEquals(Task.Status.IN_PROGRESS, maestroTask.getStatus());
    assertEquals(1, maestroTask.getPollCount());
    verify(executionDAOFacade, times(0)).updateTask(any());
    // fall back to input of executeSystemTask
    assertEquals(30, maestroTask.getCallbackAfterSeconds());
  }
}
