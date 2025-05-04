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
package com.netflix.maestro.engine.listeners;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.tasks.MaestroStartTask;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.Timeline;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class MaestroFinalFlowStatusCallbackTest extends MaestroEngineBaseTest {
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private Flow flow;
  @Mock private MaestroTask maestroTask;

  MaestroFinalFlowStatusCallback statusListener;

  @Before
  public void before() throws Exception {
    statusListener =
        new MaestroFinalFlowStatusCallback(
            maestroTask, instanceDao, stepInstanceDao, MAPPER, metricRepo);
    when(flow.getFlowId()).thenReturn("test-workflow-id");
    Task task = new Task();
    TaskDef taskDef = new TaskDef("foo", Constants.MAESTRO_TASK_NAME, null, null);
    task.setTaskDef(taskDef);
    task.setStatus(Task.Status.COMPLETED);
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "SUCCEEDED"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.SUCCEEDED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(flow.getFinishedTasks()).thenReturn(Collections.singletonList(task));
    when(flow.getInput())
        .thenReturn(
            Collections.singletonMap(
                Constants.WORKFLOW_SUMMARY_FIELD,
                threeItemMap(
                    "workflow_id",
                    "test-workflow-id",
                    "initiator",
                    singletonMap("type", "MANUAL"),
                    "runtime_dag",
                    singletonMap("bar", new StepTransition()))));
    when(flow.getReasonForIncompletion()).thenReturn("test-reason");
  }

  @After
  public void after() throws Exception {
    metricRepo.reset();
  }

  @Test
  public void testFlowCompleted() {
    statusListener.onFlowCompleted(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowCompleted")
            .count());
  }

  @Test
  public void testFlowTerminated() {
    when(flow.getStatus()).thenReturn(Flow.Status.FAILED);
    statusListener.onFlowTerminated(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowTerminated",
                "status",
                "FAILED")
            .count());
  }

  @Test
  public void testFlowTimedOut() {
    when(flow.getStatus()).thenReturn(Flow.Status.TIMED_OUT);
    statusListener.onFlowTerminated(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowTerminated",
                "status",
                "TIMED_OUT")
            .count());
  }

  @Test
  public void testFlowFinalizedComplete() {
    when(flow.getStatus()).thenReturn(Flow.Status.COMPLETED);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "COMPLETED")
            .count());

    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.SUCCEEDED);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        2L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "COMPLETED")
            .count());
  }

  @Test
  public void testFlowFinalizedCompleteOutOfSync() {
    when(flow.getStatus()).thenReturn(Flow.Status.COMPLETED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "COMPLETED")
            .count());
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "updateOnFlowFinalized",
                "status",
                "SUCCEEDED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.SUCCEEDED), anyLong(), any());
  }

  @Test
  public void testFlowFinalizedFailed() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.FATALLY_FAILED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(flow.getStatus()).thenReturn(Flow.Status.FAILED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "FAILED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.FAILED), anyLong(), any());
  }

  @Test
  public void testFlowFinalizedTimedOut() {
    when(flow.getStatus()).thenReturn(Flow.Status.TIMED_OUT);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "TIMED_OUT")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.TIMED_OUT), anyLong(), any());
  }

  @Test
  public void testFlowFinalizedTerminated() {
    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "TERMINATED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.STOPPED), anyLong(), any());
  }

  @Test
  public void testFlowFinalizedTerminatedForKilled() {
    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(flow.getReasonForIncompletion()).thenReturn("FAILED-test-reason");
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "TERMINATED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.FAILED), anyLong(), any());
  }

  @Test
  public void testFlowFinalizedForDedup() {
    when(flow.getStatus()).thenReturn(Flow.Status.FAILED);
    when(flow.getReasonForIncompletion())
        .thenReturn(MaestroStartTask.DEDUP_FAILURE_PREFIX + "test-reason");
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "FAILED")
            .count());
    verify(instanceDao, times(0))
        .updateWorkflowInstance(any(), any(), any(), any(), anyLong(), any());
  }

  @Test
  public void testFlowFinalizedCancelInternallyFailedTasks() {
    Task task = new Task();
    TaskDef taskDef = new TaskDef("bar", Constants.MAESTRO_TASK_NAME, null, null);
    task.setTaskDef(taskDef);
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "RUNNING"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));

    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.STOPPED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("bar", state));
    when(flow.getStreamOfAllTasks()).thenReturn(Stream.of(task));
    doAnswer(
            invocation -> {
              summary.put("runtime_state", Collections.singletonMap("status", "STOPPED"));
              task.setOutputData(
                  Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
              return null;
            })
        .when(maestroTask)
        .cancel(flow, task);

    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "TERMINATED")
            .count());

    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.STOPPED), anyLong(), any());
    verify(maestroTask, times(1)).cancel(flow, task);
  }

  @Test
  public void testFlowFinalizedNotCreatedTasks() {
    Task task = new Task();
    TaskDef taskDef = new TaskDef("bar", Constants.MAESTRO_TASK_NAME, null, null);
    task.setTaskDef(taskDef);

    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "CREATED"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));

    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong())).thenReturn(new HashMap<>());
    when(flow.getStreamOfAllTasks()).thenReturn(Stream.of(task));

    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "TERMINATED")
            .count());

    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.STOPPED), anyLong(), any());
    verify(maestroTask, times(1)).cancel(flow, task);
  }

  @Test
  public void testInvalidStatusOnFlowFinalized() {
    Task task = new Task();
    TaskDef taskDef = new TaskDef("foo", Constants.MAESTRO_TASK_NAME, null, null);
    task.setTaskDef(taskDef);
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "SUCCEEDED"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
    task.setStatus(Task.Status.IN_PROGRESS);
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.NOT_CREATED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(flow.getStreamOfAllTasks()).thenReturn(Stream.of(task));

    when(flow.getFlowId()).thenReturn("test-workflow-id");
    when(flow.getStatus()).thenReturn(Flow.Status.RUNNING);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "invalidStatusOnFlowFinalized")
            .count());
    ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(),
            any(),
            timelineCaptor.capture(),
            eq(WorkflowInstance.Status.FAILED),
            anyLong(),
            any());
    Timeline timeline = timelineCaptor.getValue();
    Assert.assertEquals(2, timeline.getTimelineEvents().size());
    Assert.assertEquals(
        "Workflow instance status is updated to [FAILED] due to [test-reason]",
        timeline.getTimelineEvents().get(0).getMessage());
    Assert.assertEquals(
        "Invalid status [RUNNING] onFlowFinalized",
        timeline.getTimelineEvents().get(1).getMessage());
  }

  @Test
  public void testDaoErrorOnFlowFinalized() {
    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instanceDao.updateWorkflowInstance(any(), any(), any(), any(), anyLong(), any()))
        .thenReturn(Optional.of(Details.create("test errors")));
    AssertHelper.assertThrows(
        "instance dao failure and will retry",
        MaestroRetryableError.class,
        "Failed to update workflow instance",
        () -> statusListener.onFlowFinalized(flow));
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "onFlowFinalized",
                "status",
                "TERMINATED")
            .count());
  }

  @Test
  public void testNonTerminalStatusOnFlowFinalizedError() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.RUNNING);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    AssertHelper.assertThrows(
        "Retry nonterminal status",
        MaestroRetryableError.class,
        "step status is invalid and will retry termination",
        () -> statusListener.onFlowFinalized(flow));

    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "nonTerminalStatusOnFlowFinalized")
            .count());
  }

  @Test
  public void testInconsistentStatsOnFlowFinalized() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.FATALLY_FAILED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(flow.getStatus()).thenReturn(Flow.Status.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onFlowFinalized(flow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "inconsistentStatsOnFlowFinalized")
            .count());
    ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(),
            any(),
            timelineCaptor.capture(),
            eq(WorkflowInstance.Status.STOPPED),
            anyLong(),
            any());
    Timeline timeline = timelineCaptor.getValue();
    Assert.assertEquals(1, timeline.getTimelineEvents().size());
    Assert.assertEquals(
        "Workflow instance status is updated to [STOPPED] due to [test-reason]",
        timeline.getTimelineEvents().getFirst().getMessage());
  }

  @Test
  public void testNonRetryableErrorOnFlowFinalized() {
    Task task1 = new Task();
    TaskDef taskDef1 = new TaskDef("bar", Constants.MAESTRO_TASK_NAME, null, null);
    task1.setTaskDef(taskDef1);
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "SUCCEEDED"));
    summary.put("type", "NOOP");
    task1.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
    task1.setStatus(Task.Status.FAILED);

    Task task2 = new Task();
    TaskDef taskDef2 = new TaskDef("foo", Constants.MAESTRO_TASK_NAME, null, null);
    task2.setTaskDef(taskDef2);
    task2.setOutputData(new HashMap<>());
    task2.setStatus(Task.Status.COMPLETED);

    when(flow.getStreamOfAllTasks()).thenReturn(Stream.of(task1, task2));

    when(flow.getFlowId()).thenReturn("test-workflow-id");
    when(flow.getStatus()).thenReturn(Flow.Status.RUNNING);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);

    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.NOT_CREATED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("bat", state));

    StepTransition transition1 = new StepTransition();
    transition1.setSuccessors(Collections.singletonMap("foo", "true"));
    StepTransition transition2 = new StepTransition();
    transition2.setPredecessors(Collections.singletonList("bar"));
    when(flow.getInput())
        .thenReturn(
            Collections.singletonMap(
                Constants.WORKFLOW_SUMMARY_FIELD,
                threeItemMap(
                    "workflow_id",
                    "test-workflow-id",
                    "initiator",
                    singletonMap("type", "MANUAL"),
                    "runtime_dag",
                    twoItemMap("bar", transition1, "foo", transition2))));

    statusListener.onFlowFinalized(flow);

    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.FINAL_FLOW_STATUS_CALL_BACK_METRIC,
                MaestroFinalFlowStatusCallback.class,
                "type",
                "nonRetryableErrorOnFlowFinalized")
            .count());

    ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(),
            any(),
            timelineCaptor.capture(),
            eq(WorkflowInstance.Status.FAILED),
            anyLong(),
            any());
    Timeline timeline = timelineCaptor.getValue();
    Assert.assertEquals(2, timeline.getTimelineEvents().size());
    Assert.assertEquals(
        "Workflow instance status is updated to [FAILED] due to [test-reason]",
        timeline.getTimelineEvents().get(0).getMessage());
    Assert.assertEquals(
        "Invalid state: stepId [foo] should not have any status",
        timeline.getTimelineEvents().get(1).getMessage());
  }
}
