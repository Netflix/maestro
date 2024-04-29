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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.engine.MaestroEngineBaseTest;
import com.netflix.maestro.engine.dao.MaestroStepInstanceDao;
import com.netflix.maestro.engine.dao.MaestroWorkflowInstanceDao;
import com.netflix.maestro.engine.jobevents.WorkflowInstanceUpdateJobEvent;
import com.netflix.maestro.engine.metrics.MetricConstants;
import com.netflix.maestro.engine.publisher.MaestroJobEventPublisher;
import com.netflix.maestro.engine.tasks.MaestroStartTask;
import com.netflix.maestro.engine.tasks.MaestroTask;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.error.Details;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.StepRuntimeState;
import com.netflix.maestro.models.instance.WorkflowInstance;
import com.netflix.maestro.models.timeline.Timeline;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

public class MaestroWorkflowStatusListenerTest extends MaestroEngineBaseTest {
  @Mock private MaestroWorkflowInstanceDao instanceDao;
  @Mock private MaestroStepInstanceDao stepInstanceDao;
  @Mock private MaestroJobEventPublisher publisher;
  @Mock private Workflow workflow;
  @Mock private MaestroTask maestroTask;

  MaestroWorkflowStatusListener statusListener;

  @Before
  public void before() throws Exception {
    statusListener =
        new MaestroWorkflowStatusListener(
            maestroTask, instanceDao, stepInstanceDao, publisher, MAPPER, metricRepo);
    when(workflow.getWorkflowId()).thenReturn("test-workflow-id");
    Task task = new Task();
    task.setReferenceTaskName("foo");
    task.setStatus(Task.Status.COMPLETED);
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "SUCCEEDED"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
    task.setTaskType(Constants.MAESTRO_TASK_NAME);
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.SUCCEEDED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(workflow.getTasks()).thenReturn(Collections.singletonList(task));
    when(workflow.getInput())
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
    when(workflow.getReasonForIncompletion()).thenReturn("test-reason");
  }

  @After
  public void after() throws Exception {
    metricRepo.reset();
  }

  @Test
  public void testWorkflowCompleted() {
    statusListener.onWorkflowCompleted(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowCompleted")
            .count());
  }

  @Test
  public void testWorkflowTerminated() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.FAILED);
    statusListener.onWorkflowTerminated(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowTerminated",
                "status",
                "FAILED")
            .count());
  }

  @Test
  public void testWorkflowTimedOut() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TIMED_OUT);
    statusListener.onWorkflowTerminated(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowTerminated",
                "status",
                "TIMED_OUT")
            .count());
  }

  @Test
  public void testWorkflowFinalizedComplete() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.COMPLETED);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "COMPLETED")
            .count());

    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.SUCCEEDED);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        2L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "COMPLETED")
            .count());
  }

  @Test
  public void testWorkflowFinalizedCompleteOutOfSync() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.COMPLETED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "COMPLETED")
            .count());
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "updateOnWorkflowFinalized",
                "status",
                "SUCCEEDED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.SUCCEEDED), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testWorkflowFinalizedFailed() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.FATALLY_FAILED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.FAILED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "FAILED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(any(), any(), any(), eq(WorkflowInstance.Status.FAILED), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testWorkflowFinalizedTimedOut() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TIMED_OUT);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TIMED_OUT")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.TIMED_OUT), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testWorkflowFinalizedTerminated() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TERMINATED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.STOPPED), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testWorkflowFinalizedTerminatedForKilled() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(workflow.getReasonForIncompletion()).thenReturn("FAILED-test-reason");
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TERMINATED")
            .count());
    verify(instanceDao, times(1))
        .updateWorkflowInstance(any(), any(), any(), eq(WorkflowInstance.Status.FAILED), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testWorkflowFinalizedForDedup() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.FAILED);
    when(workflow.getReasonForIncompletion())
        .thenReturn(MaestroStartTask.DEDUP_FAILURE_PREFIX + "test-reason");
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "FAILED")
            .count());
    verify(instanceDao, times(0)).updateWorkflowInstance(any(), any(), any(), any(), anyLong());
    verify(publisher, times(0)).publishOrThrow(any(), any());
  }

  @Test
  public void testWorkflowFinalizedCancelInternallyFailedTasks() {
    Task task = new Task();
    task.setReferenceTaskName("bar");

    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "RUNNING"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));

    task.setTaskType(Constants.MAESTRO_TASK_NAME);
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.STOPPED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("bar", state));
    when(workflow.getTasks()).thenReturn(Collections.singletonList(task));
    doAnswer(
            invocation -> {
              summary.put("runtime_state", Collections.singletonMap("status", "STOPPED"));
              task.setOutputData(
                  Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
              return null;
            })
        .when(maestroTask)
        .cancel(workflow, task, null);

    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TERMINATED")
            .count());

    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.STOPPED), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(), any());
    verify(maestroTask, times(1)).cancel(workflow, task, null);
  }

  @Test
  public void testWorkflowFinalizedNotCreatedTasks() {
    Task task = new Task();
    task.setReferenceTaskName("bar");

    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "CREATED"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));

    task.setTaskType(Constants.MAESTRO_TASK_NAME);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong())).thenReturn(new HashMap<>());
    when(workflow.getTasks()).thenReturn(Collections.singletonList(task));

    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TERMINATED")
            .count());

    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), any(), eq(WorkflowInstance.Status.STOPPED), anyLong());
    verify(publisher, times(1)).publishOrThrow(any(WorkflowInstanceUpdateJobEvent.class), any());
    verify(maestroTask, times(1)).cancel(workflow, task, null);
  }

  @Test
  public void testInvalidStatusOnWorkflowFinalized() {
    Task task = new Task();
    task.setReferenceTaskName("foo");
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "SUCCEEDED"));
    summary.put("type", "NOOP");
    task.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
    task.setTaskType(Constants.MAESTRO_TASK_NAME);
    task.setStatus(Task.Status.IN_PROGRESS);
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.NOT_CREATED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(workflow.getTasks()).thenReturn(Collections.singletonList(task));

    when(workflow.getWorkflowId()).thenReturn("test-workflow-id");
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.PAUSED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "invalidStatusOnWorkflowFinalized")
            .count());
    ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), timelineCaptor.capture(), eq(WorkflowInstance.Status.FAILED), anyLong());
    Timeline timeline = timelineCaptor.getValue();
    Assert.assertEquals(2, timeline.getTimelineEvents().size());
    Assert.assertEquals(
        "Workflow instance status is updated to [FAILED] due to [test-reason]",
        timeline.getTimelineEvents().get(0).getMessage());
    Assert.assertEquals(
        "Invalid status [PAUSED] onWorkflowFinalized",
        timeline.getTimelineEvents().get(1).getMessage());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testDaoErrorOnWorkflowFinalized() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    when(instanceDao.updateWorkflowInstance(any(), any(), any(), any(), anyLong()))
        .thenReturn(Optional.of(Details.create("test errors")));
    AssertHelper.assertThrows(
        "instance dao failure and will retry",
        MaestroRetryableError.class,
        "Failed to update workflow instance",
        () -> statusListener.onWorkflowFinalized(workflow));
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TERMINATED")
            .count());
  }

  @Test
  public void testPublishErrorOnWorkflowFinalized() {
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    doCallRealMethod().when(publisher).publishOrThrow(any(), any());
    doCallRealMethod().when(publisher).publishOrThrow(any(), anyLong(), any());
    when(publisher.publish(any(), anyLong()))
        .thenReturn(Optional.of(Details.create("test errors")));
    AssertHelper.assertThrows(
        "maestro event publish failure and will retry",
        MaestroRetryableError.class,
        "Failed to publish maestro job event",
        () -> statusListener.onWorkflowFinalized(workflow));
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "onWorkflowFinalized",
                "status",
                "TERMINATED")
            .count());
  }

  @Test
  public void testNonTerminalStatusOnWorkflowFinalizedError() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.RUNNING);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    AssertHelper.assertThrows(
        "Retry nonterminal status",
        MaestroRetryableError.class,
        "step status is invalid and will retry termination",
        () -> statusListener.onWorkflowFinalized(workflow));

    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "nonTerminalStatusOnWorkflowFinalized")
            .count());
  }

  @Test
  public void testInconsistentStatsOnWorkflowFinalized() {
    StepRuntimeState state = new StepRuntimeState();
    state.setStatus(StepInstance.Status.FATALLY_FAILED);
    when(stepInstanceDao.getAllStepStates(any(), anyLong(), anyLong()))
        .thenReturn(singletonMap("foo", state));
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.TERMINATED);
    when(instanceDao.getWorkflowInstanceStatus(eq("test-workflow-id"), anyLong(), anyLong()))
        .thenReturn(WorkflowInstance.Status.IN_PROGRESS);
    statusListener.onWorkflowFinalized(workflow);
    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "inconsistentStatsOnWorkflowFinalized")
            .count());
    ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), timelineCaptor.capture(), eq(WorkflowInstance.Status.STOPPED), anyLong());
    Timeline timeline = timelineCaptor.getValue();
    Assert.assertEquals(1, timeline.getTimelineEvents().size());
    Assert.assertEquals(
        "Workflow instance status is updated to [STOPPED] due to [test-reason]",
        timeline.getTimelineEvents().get(0).getMessage());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }

  @Test
  public void testNonRetryableErrorOnWorkflowFinalized() {
    Task task1 = new Task();
    task1.setReferenceTaskName("bar");
    Map<String, Object> summary = new HashMap<>();
    summary.put("runtime_state", Collections.singletonMap("status", "SUCCEEDED"));
    summary.put("type", "NOOP");
    task1.setOutputData(Collections.singletonMap(Constants.STEP_RUNTIME_SUMMARY_FIELD, summary));
    task1.setTaskType(Constants.MAESTRO_TASK_NAME);
    task1.setStatus(Task.Status.FAILED);

    Task task2 = new Task();
    task2.setReferenceTaskName("foo");
    task2.setTaskType(Constants.MAESTRO_TASK_NAME);
    task2.setStatus(Task.Status.COMPLETED);

    when(workflow.getTasks()).thenReturn(Arrays.asList(task1, task2));

    when(workflow.getWorkflowId()).thenReturn("test-workflow-id");
    when(workflow.getStatus()).thenReturn(Workflow.WorkflowStatus.PAUSED);
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
    when(workflow.getInput())
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

    statusListener.onWorkflowFinalized(workflow);

    Assert.assertEquals(
        1L,
        metricRepo
            .getCounter(
                MetricConstants.WORKFLOW_STATUS_LISTENER_CALL_BACK_METRIC,
                MaestroWorkflowStatusListener.class,
                "type",
                "nonRetryableErrorOnWorkflowFinalized")
            .count());

    ArgumentCaptor<Timeline> timelineCaptor = ArgumentCaptor.forClass(Timeline.class);
    verify(instanceDao, times(1))
        .updateWorkflowInstance(
            any(), any(), timelineCaptor.capture(), eq(WorkflowInstance.Status.FAILED), anyLong());
    Timeline timeline = timelineCaptor.getValue();
    Assert.assertEquals(2, timeline.getTimelineEvents().size());
    Assert.assertEquals(
        "Workflow instance status is updated to [FAILED] due to [test-reason]",
        timeline.getTimelineEvents().get(0).getMessage());
    Assert.assertEquals(
        "Invalid state: stepId [foo] should not have any status",
        timeline.getTimelineEvents().get(1).getMessage());
    verify(publisher, times(1)).publishOrThrow(any(), any());
  }
}
