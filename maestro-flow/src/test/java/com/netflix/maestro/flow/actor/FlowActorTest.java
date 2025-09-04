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
package com.netflix.maestro.flow.actor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class FlowActorTest extends ActorBaseTest {

  private GroupActor groupActor;
  private FlowActor flowActor;
  private Flow flow;

  @Before
  public void setUp() {
    when(properties.getFlowRefreshIntervalInMillis()).thenReturn(3000L);
    groupActor = createGroupActor();
    flow = createFlow();
    flowActor = new FlowActor(flow, groupActor, context);
  }

  @Test
  public void testBeforeRunning() {
    flowActor.beforeRunning();
    verify(context, times(1)).getMetrics();
    verify(metrics, times(1)).counter("num_of_running_flows", FlowActor.class);
  }

  @Test
  public void testRunForActionFlowStart() {
    assertNull(flow.getPrepareTask());
    assertNull(flow.getMonitorTask());

    flowActor.runForAction(Action.FLOW_START);
    assertNotNull(flow.getPrepareTask());
    assertNotNull(flow.getMonitorTask());
    verify(context, times(0)).resumeFlow(flow);
    verify(context, times(1)).prepare(flow);
    assertEquals(Set.of(Action.FLOW_TIMEOUT), flowActor.getScheduledActions().keySet());

    var actions = flowActor.getActions();
    assertEquals(1, actions.size());
    assertEquals(Action.FlowReconcile.class, actions.poll().getClass());
  }

  @Test
  public void testRunForActionFlowResumeWithRunningFlow() {
    assertNull(flow.getPrepareTask());
    assertNull(flow.getMonitorTask());

    flowActor.runForAction(Action.FLOW_RESUME);
    assertNull(flow.getPrepareTask());
    assertNull(flow.getMonitorTask());
    verify(context, times(1)).resumeFlow(flow);
    verify(context, times(0)).prepare(flow);
    assertEquals(Set.of(Action.FLOW_TIMEOUT), flowActor.getScheduledActions().keySet());

    var actions = flowActor.getActions();
    assertEquals(1, actions.size());
    assertEquals(Action.FlowReconcile.class, actions.poll().getClass());
  }

  @Test
  public void testRunForActionFlowResumeWithRunningTasks() {
    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    Task task2 = flow.newTask(new TaskDef("task2", "noop", null), false);
    task2.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task2);

    flowActor.runForAction(Action.FLOW_RESUME);
    var actions = flowActor.getActions();
    assertEquals(2, actions.size());
    assertEquals(new Action.FlowTaskRetry("task2"), actions.poll());
    assertEquals(Action.FlowReconcile.class, actions.poll().getClass());

    verify(context, times(1)).resumeFlow(flow);
    verify(context, times(1)).cloneTask(task1);
    verify(context, times(1)).run(any());
    verify(context, times(0)).prepare(flow);
    assertEquals(Set.of(Action.FLOW_TIMEOUT), flowActor.getScheduledActions().keySet());

    assertTrue(flowActor.containsChild("task1"));
    var child = flowActor.getChild("task1");
    verifyActions(child, Action.TASK_RESUME);
  }

  @Test
  public void testRunForActionFlowResumeWithTerminatedFlow() {
    flow.setStatus(Flow.Status.FAILED);
    assertNull(flow.getPrepareTask());
    assertNull(flow.getMonitorTask());

    flowActor.runForAction(Action.FLOW_RESUME);
    assertNull(flow.getPrepareTask());
    assertNull(flow.getMonitorTask());
    assertFalse(flowActor.isRunning());

    flowActor.afterRunning();
    verify(context, times(1)).deleteFlow(flow);
    verify(context, times(1)).resumeFlow(flow);
    verify(context, times(0)).prepare(flow);
    verifyEmptyAction(flowActor);
  }

  @Test
  public void testRunForActionFlowReconcile() {
    flow.getFlowDef()
        .setTasks(
            List.of(
                List.of(new TaskDef("task1", "noop", null)),
                List.of(new TaskDef("task2", "noop", null))));
    flow.setPrepareTask(flow.newTask(new TaskDef("prepare", "noop", null), true));
    flow.setMonitorTask(flow.newTask(new TaskDef("monitor", "noop", null), true));

    flowActor.runForAction(new Action.FlowReconcile(123));
    assertEquals(2, flow.getRunningTasks().size());
    assertTrue(flowActor.containsChild("task1"));
    assertTrue(flowActor.containsChild("task2"));
    var child1 = flowActor.getChild("task1");
    var child2 = flowActor.getChild("task2");
    verifyActions(child1, Action.TASK_START);
    verifyActions(child2, Action.TASK_START);

    verify(context, times(2)).cloneTask(any());
    verify(context, times(1)).refresh(flow);
    verify(context, times(0)).finalCall(flow);

    var scheduledActions = flowActor.getScheduledActions();
    assertEquals(1, scheduledActions.size());
    assertEquals(
        Action.FlowReconcile.class,
        scheduledActions.keySet().stream().findFirst().get().getClass());
  }

  @Test
  public void testRunForActionFlowReconcileWithFailedPreparedTask() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    flow.getFlowDef()
        .setTasks(
            List.of(
                List.of(new TaskDef("task1", "noop", null)),
                List.of(new TaskDef("task2", "noop", null))));
    flow.setPrepareTask(flow.newTask(new TaskDef("prepare", "noop", null), true));
    flow.setMonitorTask(flow.newTask(new TaskDef("monitor", "noop", null), true));

    flowActor.runForAction(new Action.FlowReconcile(123));
    var child1 = flowActor.getChild("task1");
    var child2 = flowActor.getChild("task2");
    verifyActions(child1, Action.TASK_START);
    verifyActions(child2, Action.TASK_START);
    assertTrue(flowActor.isRunning());
    verify(context, times(2)).cloneTask(any());
    verify(context, times(1)).refresh(flow);

    flow.getPrepareTask().setStatus(Task.Status.FAILED_WITH_TERMINAL_ERROR);
    flowActor.runForAction(new Action.FlowReconcile(123));
    assertEquals(Task.Status.CANCELED, flow.getMonitorTask().getStatus());
    assertEquals(Flow.Status.FAILED, flow.getStatus());
    verify(future, times(1)).cancel(true);
    verifyActions(child1, Action.TASK_PING);
    verifyActions(child2, Action.TASK_PING);
    verify(context, times(1)).finalCall(flow);
    assertFalse(flowActor.isRunning());
  }

  @Test
  public void testFlowRefreshRunningFlow() {
    flow.setMonitorTask(flow.newTask(new TaskDef("monitor", "noop", null), true));
    flowActor.runForAction(Action.FLOW_REFRESH);
    verify(context, times(1)).refresh(flow);
    verify(context, times(0)).finalCall(flow);
  }

  @Test
  public void testFlowRefreshTerminatedFlow() {
    flow.setPrepareTask(flow.newTask(new TaskDef("prepare", "noop", null), true));
    flow.setMonitorTask(flow.newTask(new TaskDef("monitor", "noop", null), true));
    flow.getMonitorTask().setStatus(Task.Status.COMPLETED);

    flowActor.runForAction(Action.FLOW_REFRESH);
    verify(context, times(1)).refresh(flow);
    verify(context, times(1)).finalCall(flow);
    assertEquals(Flow.Status.COMPLETED, flow.getStatus());
    assertFalse(flowActor.isRunning());
  }

  @Test
  public void testFlowTaskRetry() {
    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task1);

    flowActor.runForAction(new Action.FlowTaskRetry("task1"));
    assertTrue(flowActor.containsChild("task1"));
    var child = flowActor.getChild("task1");
    verifyActions(child, Action.TASK_START);
    assertEquals(Set.of("task1"), flow.getRunningTasks().keySet());
    verify(context, times(1)).cloneTask(any());
    verify(context, times(1)).run(any());
  }

  @Test
  public void testTaskUpdateForRunningTask() {
    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task1);
    flowActor.runForAction(new Action.FlowTaskRetry("task1"));

    Task task2 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task2.setStatus(Task.Status.IN_PROGRESS);

    flowActor.runForAction(new Action.TaskUpdate(task2));
    assertEquals(Set.of(Action.FLOW_REFRESH), flowActor.getScheduledActions().keySet());
    assertEquals(task2, flow.getRunningTasks().get("task1"));
    verifyEmptyAction(flowActor);
  }

  @Test
  public void testTaskUpdateForRunningInactiveTask() {
    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task1);
    flow.setMonitorTask(flow.newTask(new TaskDef("monitor", "noop", null), true));

    flowActor.runForAction(new Action.FlowTaskRetry("task1"));
    Task task2 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task2.setStatus(Task.Status.IN_PROGRESS);
    task2.setActive(false);
    task2.setStartDelayInMillis(3000000);

    flowActor.runForAction(new Action.TaskUpdate(task2));
    assertEquals(
        Set.of(Action.FLOW_REFRESH, new Action.TaskWakeUp("task1")),
        flowActor.getScheduledActions().keySet());
    assertEquals(task2, flow.getRunningTasks().get("task1"));
  }

  @Test
  public void testTaskUpdateForTerminatedTask() {
    flow.getFlowDef()
        .setTasks(
            List.of(
                List.of(new TaskDef("task1", "noop", null)),
                List.of(new TaskDef("task2", "noop", null))));
    flow.setPrepareTask(flow.newTask(new TaskDef("prepare", "noop", null), true));
    flow.setMonitorTask(flow.newTask(new TaskDef("monitor", "noop", null), true));

    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task1);
    flowActor.runForAction(new Action.FlowTaskRetry("task1"));
    verify(context, times(1)).cloneTask(any());

    Task task2 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task2.setStatus(Task.Status.FAILED);
    task2.setStartDelayInMillis(3000000);
    flowActor.runForAction(new Action.TaskUpdate(task2));
    assertEquals(
        Set.of(new Action.FlowTaskRetry("task1")), flowActor.getScheduledActions().keySet());
    assertFalse(flowActor.containsChild("task1"));
    assertFalse(flow.getRunningTasks().containsKey("task1"));
    assertTrue(flowActor.containsChild("task2"));
    assertTrue(flow.getRunningTasks().containsKey("task2"));
    var child2 = flowActor.getChild("task2");
    verifyActions(child2, Action.TASK_START);

    verify(context, times(2)).cloneTask(any());
    verify(context, times(1)).refresh(flow);
    verify(context, times(0)).finalCall(flow);
  }

  @Test
  public void testTaskWakeUpWithRunningTask() {
    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    flow.addFinishedTask(task1);
    flowActor.runForAction(new Action.FlowTaskRetry("task1"));
    verifyActions(flowActor.getChild("task1"), Action.TASK_START);

    Task task2 = flow.getRunningTasks().get("task1");
    task2.setActive(false);
    flowActor.runForAction(new Action.TaskWakeUp("task1"));
    assertTrue(task2.isActive());
    verifyActions(flowActor.getChild("task1"), Action.TASK_ACTIVATE);
  }

  @Test
  public void testTaskWakeUpWithQueuedTask() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);
    when(future.cancel(false)).thenReturn(true);

    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    task1.setStartDelayInMillis(3000000);
    flow.addFinishedTask(task1);
    flowActor.runForAction(Action.FLOW_RESUME);
    assertFalse(flowActor.containsChild("task1"));

    flowActor.runForAction(new Action.TaskWakeUp("task1"));
    verify(future, times(1)).cancel(false);
    verifyActions(flowActor.getChild("task1"), Action.TASK_START, Action.TASK_ACTIVATE);
    assertTrue(flowActor.containsChild("task1"));
    verify(context, times(1)).getMetrics();
    verify(metrics, times(1)).counter("num_of_wakeup_flows", FlowActor.class, "forall", "false");
  }

  @Test
  public void testTaskWakeUpForAllFlowTasks() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);
    when(future.cancel(false)).thenReturn(true);

    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    task1.setStartDelayInMillis(3000000);
    flow.addFinishedTask(task1);
    Task task2 = flow.newTask(new TaskDef("task2", "noop", null), false);
    task2.setStatus(Task.Status.IN_PROGRESS);
    flow.updateRunningTask(task2);
    flowActor.runForAction(Action.FLOW_RESUME);
    assertFalse(flowActor.containsChild("task1"));
    assertTrue(flowActor.containsChild("task2"));

    flowActor.runForAction(new Action.TaskWakeUp(null));
    verify(future, times(1)).cancel(false);
    verifyActions(flowActor.getChild("task1"), Action.TASK_START, Action.TASK_PING);
    verifyActions(flowActor.getChild("task2"), Action.TASK_RESUME, Action.TASK_PING);
    assertTrue(flowActor.containsChild("task1"));
    assertTrue(flowActor.containsChild("task2"));
    verify(context, times(1)).getMetrics();
    verify(metrics, times(1)).counter("num_of_wakeup_flows", FlowActor.class, "forall", "true");
  }

  @Test
  public void testFlowTimeoutWithoutTasks() {
    flowActor.runForAction(Action.FLOW_TIMEOUT);
    assertEquals(Flow.Status.TIMED_OUT, flow.getStatus());
    assertEquals(Set.of(Action.FLOW_TIMEOUT), flowActor.getScheduledActions().keySet());
  }

  @Test
  public void testFlowTimeoutWithTasks() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);
    when(future.cancel(false)).thenReturn(true);

    Task task1 = flow.newTask(new TaskDef("task1", "noop", null), false);
    task1.setStatus(Task.Status.FAILED);
    task1.setStartDelayInMillis(3000000);
    flow.addFinishedTask(task1);
    flowActor.runForAction(Action.FLOW_RESUME);
    assertFalse(flowActor.containsChild("task1"));

    flowActor.runForAction(Action.FLOW_TIMEOUT);
    assertEquals(Flow.Status.TIMED_OUT, flow.getStatus());
    assertEquals(
        Set.of(new Action.FlowTaskRetry("task1"), Action.FLOW_TIMEOUT),
        flowActor.getScheduledActions().keySet());
    verifyActions(flowActor.getChild("task1"), Action.TASK_START, Action.TASK_STOP);
  }

  @Test
  public void testFlowShutdown() {
    flowActor.runForAction(Action.FLOW_SHUTDOWN);
    verifyActions(groupActor, Action.FLOW_DOWN);
    assertFalse(flowActor.isRunning());
  }

  @Test
  public void testTaskDown() {
    flowActor.runForAction(Action.TASK_DOWN);
    verifyActions(groupActor, Action.FLOW_DOWN);
    assertFalse(flowActor.isRunning());
  }

  @Test
  public void testUnexpectedAction() {
    AssertHelper.assertThrows(
        "should throw for unexpected action",
        MaestroUnprocessableEntityException.class,
        "Unexpected action: [TaskPing[]] for flow ",
        () -> flowActor.runForAction(Action.TASK_PING));
  }

  @Test
  public void testAfterRunning() {
    flowActor.afterRunning();
    verify(context, times(1)).getMetrics();
    verify(metrics, times(1))
        .counter("num_of_finished_flows", FlowActor.class, "finalized", "false");
    verify(context, times(0)).deleteFlow(any());
  }

  @Test
  public void testReference() {
    assertEquals("test-flow-ref", flowActor.reference());
  }

  @Test
  public void testName() {
    assertEquals("test-flow-ref", flowActor.name());
  }
}
