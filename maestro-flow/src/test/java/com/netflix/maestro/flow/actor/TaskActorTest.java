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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.Task;
import com.netflix.maestro.flow.models.TaskDef;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class TaskActorTest extends ActorBaseTest {

  private Flow flow;
  private Task task;
  private FlowActor flowActor;
  private TaskActor taskActor;

  @Before
  public void setUp() {
    flow = createFlow();
    task = flow.newTask(new TaskDef("task1", "noop", null, null), false);
    flowActor = new FlowActor(flow, createGroupActor(), context);
    taskActor = new TaskActor(task, flow, flowActor, context);
  }

  @Test
  public void testBeforeRunning() {
    taskActor.beforeRunning();
    verify(context, times(1)).getMetrics();
    verify(metrics, times(1)).counter("num_of_running_tasks", TaskActor.class);
  }

  @Test
  public void testRunForActionTaskStart() {
    taskActor.runForAction(Action.TASK_START);
    verify(context, times(1)).start(any(), any());
    verifyActions(taskActor, Action.TASK_PING);
  }

  @Test
  public void testRunForActionTaskResume() {
    taskActor.runForAction(Action.TASK_RESUME);
    verify(context, times(0)).start(any(), any());
    verifyActions(taskActor, Action.TASK_PING);
  }

  @Test
  public void testRunForActionStopForActiveRunningTask() {
    taskActor.runForAction(Action.TASK_STOP);
    verify(context, times(1)).cancel(any(), any());
    assertEquals(Set.of(Action.TASK_STOP), taskActor.getScheduledActions().keySet());
    verifyEmptyAction(taskActor);
    assertTrue(taskActor.isRunning());
  }

  @Test
  public void testRunForActionStopForInactiveTask() {
    task.setActive(false);

    taskActor.runForAction(Action.TASK_STOP);
    verify(context, times(0)).cancel(any(), any());
  }

  @Test
  public void testRunForActionStopForTerminatedTask() {
    task.setStatus(Task.Status.FAILED);

    taskActor.runForAction(Action.TASK_STOP);
    verify(context, times(1)).cancel(any(), any());
    assertTrue(taskActor.getScheduledActions().isEmpty());
    verifyEmptyAction(taskActor);
    assertFalse(taskActor.isRunning());
    verify(context, times(1)).cloneTask(any());
    verifyActions(flowActor, new Action.TaskUpdate(task));
  }

  @Test
  public void testRunForActionTaskPingForActiveRunningTaskWithChange() {
    task.setStartTime(System.currentTimeMillis());
    task.setTimeoutInMillis(3600000L);
    task.setStartDelayInSeconds(3000);
    when(context.execute(flow, task)).thenReturn(true);

    taskActor.runForAction(Action.TASK_PING);
    verify(context, times(1)).execute(any(), any());
    assertEquals(
        Set.of(Action.TASK_TIMEOUT, Action.TASK_PING), taskActor.getScheduledActions().keySet());
    assertNull(task.getTimeoutInMillis());
    assertEquals(1, task.getPollCount());
    verify(context, times(1)).cloneTask(any());
    verifyActions(flowActor, new Action.TaskUpdate(task));
  }

  @Test
  public void testRunForActionTaskPingForInactiveRunningTaskWithoutChange() {
    task.setActive(false);

    taskActor.runForAction(Action.TASK_PING);
    verify(context, times(0)).execute(any(), any());
    assertTrue(taskActor.getScheduledActions().isEmpty());
    assertEquals(0, task.getPollCount());
    verify(context, times(0)).cloneTask(any());
    verifyEmptyAction(flowActor);
  }

  @Test
  public void testRunForActionTaskPingForTerminatedTask() {
    task.setStatus(Task.Status.FAILED);
    task.setStartDelayInSeconds(3000);
    when(context.execute(flow, task)).thenReturn(false);

    taskActor.runForAction(Action.TASK_PING);
    verify(context, times(1)).execute(any(), any());
    assertFalse(taskActor.isRunning());
    verify(context, times(1)).cloneTask(any());
    verifyActions(flowActor, new Action.TaskUpdate(task));
    verifyEmptyAction(taskActor);
    assertTrue(taskActor.getScheduledActions().isEmpty());
  }

  @Test
  public void testRunForActionTaskActivate() {
    verifyExecute(Action.TASK_ACTIVATE, false);
  }

  private void verifyExecute(Action action, boolean activeFlag) {
    task.setActive(activeFlag);
    task.setStartDelayInSeconds(3000L);
    when(context.execute(flow, task)).thenReturn(false);

    taskActor.runForAction(action);
    assertTrue(task.isActive());
    verify(context, times(1)).execute(any(), any());
    assertEquals(1, task.getPollCount());
    assertEquals(Set.of(Action.TASK_PING), taskActor.getScheduledActions().keySet());
    verify(context, times(0)).cloneTask(any());
    verifyEmptyAction(flowActor);
  }

  @Test
  public void testRunForActionTaskTimeout() {
    verifyExecute(Action.TASK_TIMEOUT, true);
  }

  @Test
  public void testRunForActionTaskShutdown() {
    taskActor.runForAction(Action.TASK_SHUTDOWN);
    assertFalse(taskActor.isRunning());
    verifyActions(flowActor, Action.TASK_DOWN);
  }

  @Test
  public void testAfterRunning() {
    taskActor.afterRunning();
    verify(context, times(1)).getMetrics();
    verify(metrics, times(1)).counter("num_of_finished_tasks", TaskActor.class);
  }

  @Test
  public void testReference() {
    assertEquals("task1", taskActor.reference());
  }

  @Test
  public void testName() {
    assertEquals("test-flow-ref[task1]", taskActor.name());
  }
}
