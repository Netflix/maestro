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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BaseActorTest extends ActorBaseTest {
  private GroupActor groupActor;
  private FlowActor flowActor;

  @Before
  public void setUp() {
    groupActor = createGroupActor();
    flowActor = new FlowActor(createFlow(), groupActor, context);
  }

  @Test
  public void testIsRunning() {
    assertTrue(groupActor.isRunning());
    assertTrue(flowActor.isRunning());

    flowActor.terminateNow();
    assertTrue(groupActor.isRunning());
    assertFalse(flowActor.isRunning());

    var actor3 = new FlowActor(createFlow(), groupActor, context);
    assertTrue(groupActor.isRunning());
    assertTrue(actor3.isRunning());

    groupActor.terminateNow();
    assertFalse(groupActor.isRunning());
    assertFalse(actor3.isRunning());
  }

  @Test
  public void testPost() {
    verifyEmptyAction(groupActor);

    groupActor.post(Action.GROUP_START);
    verifyActions(groupActor, Action.GROUP_START);
  }

  @Test
  public void testPostActionDeduplication() {
    Map<Action, Boolean> queuedActions = groupActor.getQueuedActions();
    verifyEmptyAction(groupActor);

    groupActor.post(Action.GROUP_START);
    assertEquals(1, queuedActions.size());
    assertTrue(queuedActions.containsKey(Action.GROUP_START));

    groupActor.post(Action.GROUP_HEARTBEAT);
    assertEquals(2, queuedActions.size());
    assertTrue(queuedActions.containsKey(Action.GROUP_START));
    assertTrue(queuedActions.containsKey(Action.GROUP_HEARTBEAT));

    groupActor.post(Action.GROUP_START);
    assertEquals(2, queuedActions.size());

    verifyActions(groupActor, Action.GROUP_START, Action.GROUP_HEARTBEAT);
  }

  @Test
  public void testGeneration() {
    assertEquals(2, groupActor.generation());
    assertEquals(2, flowActor.generation());
  }

  @Test
  public void testValidUntil() {
    assertEquals(12345, groupActor.validUntil());
    assertEquals(12345, flowActor.validUntil());
  }

  @Test
  public void testRunActionFor() {
    groupActor.runActionFor(flowActor, Action.FLOW_START);

    verify(context, times(1)).run(any());
    verifyActions(flowActor, Action.FLOW_START);
    assertEquals(flowActor, groupActor.getChild("test-flow-ref"));

    doThrow(new RejectedExecutionException()).when(context).run(flowActor);
    groupActor.runActionFor(flowActor, Action.FLOW_START);
    verifyEmptyAction(flowActor);

    doThrow(new RuntimeException("test unhandled error")).when(context).run(flowActor);
    AssertHelper.assertThrows(
        "should throw instead of ignore the error",
        RuntimeException.class,
        "test unhandled error",
        () -> groupActor.runActionFor(flowActor, Action.FLOW_START));
  }

  @Test
  public void testWakeUpChildActors() {
    groupActor.wakeUpChildActors(Action.FLOW_TIMEOUT);
    verifyEmptyAction(flowActor);

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    groupActor.wakeUpChildActors(Action.FLOW_TIMEOUT);
    verifyActions(flowActor, Action.FLOW_START, Action.FLOW_TIMEOUT);
  }

  @Test
  public void testWakeUpChildActor() {
    groupActor.wakeUpChildActor("test-flow-ref", Action.FLOW_TIMEOUT);
    verifyEmptyAction(flowActor);

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    groupActor.wakeUpChildActor("test-flow-ref", Action.FLOW_TIMEOUT);
    verifyActions(flowActor, Action.FLOW_START, Action.FLOW_TIMEOUT);

    groupActor.wakeUpChildActor("not-exist-flow-ref", Action.FLOW_TIMEOUT);
    verifyEmptyAction(flowActor);

    flowActor.terminateNow();
    groupActor.wakeUpChildActor("test-flow-ref", Action.FLOW_TIMEOUT);
    verifyEmptyAction(flowActor);
  }

  @Test
  public void testContainChild() {
    assertFalse(groupActor.containsChild("test-flow-ref"));

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertTrue(groupActor.containsChild("test-flow-ref"));
  }

  @Test
  public void testGetChild() {
    assertNull(groupActor.getChild("test-flow-ref"));

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertEquals(flowActor, groupActor.getChild("test-flow-ref"));
  }

  @Test
  public void testRemoveChild() {
    assertNull(groupActor.removeChild("test-flow-ref"));

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertEquals(flowActor, groupActor.removeChild("test-flow-ref"));
    assertNull(groupActor.getChild("test-flow-ref"));
  }

  @Test
  public void testCleanupChildActors() {
    groupActor.runActionFor(flowActor, Action.FLOW_START);
    groupActor.cleanupChildActors();
    assertTrue(groupActor.containsChild("test-flow-ref"));

    flowActor.terminateNow();
    groupActor.cleanupChildActors();
    assertFalse(groupActor.containsChild("test-flow-ref"));
  }

  @Test
  public void testNoChildActorsRunning() {
    assertTrue(groupActor.noChildActorsRunning());

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertFalse(groupActor.noChildActorsRunning());

    flowActor.terminateNow();
    assertTrue(groupActor.noChildActorsRunning());
  }

  @Test
  public void testStartShutdownFromRootWithoutChildActors() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    assertTrue(groupActor.isRunning());

    groupActor.schedule(Action.GROUP_START, 10000);
    assertEquals(Set.of(Action.GROUP_START), groupActor.getScheduledActions().keySet());

    groupActor.startShutdown(Action.FLOW_SHUTDOWN);
    verify(future, times(1)).cancel(true);
    assertFalse(groupActor.isRunning());
  }

  @Test
  public void testStartShutdownFromRootWithChildActors() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertTrue(groupActor.isRunning());

    groupActor.schedule(Action.GROUP_START, 10000);
    assertEquals(Set.of(Action.GROUP_START), groupActor.getScheduledActions().keySet());

    groupActor.startShutdown(Action.FLOW_SHUTDOWN);
    verify(future, times(1)).cancel(true);

    assertTrue(groupActor.isRunning());
    verifyActions(flowActor, Action.FLOW_START, Action.FLOW_SHUTDOWN);
  }

  @Test
  public void testStartShutdownFromChildWithoutChildActors() {
    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertTrue(groupActor.isRunning());
    assertTrue(flowActor.isRunning());

    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    flowActor.schedule(Action.FLOW_REFRESH, 10000);
    assertEquals(Set.of(Action.FLOW_REFRESH), flowActor.getScheduledActions().keySet());

    flowActor.startShutdown(Action.TASK_SHUTDOWN);
    verify(future, times(1)).cancel(true);
    assertFalse(flowActor.isRunning());
    assertTrue(groupActor.isRunning());
    verifyActions(groupActor, Action.FLOW_DOWN);

    groupActor.startShutdown(Action.FLOW_SHUTDOWN);
    assertFalse(groupActor.isRunning());
  }

  @Test
  public void testCheckShutdownWithChildRunning() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertTrue(groupActor.isRunning());
    assertTrue(flowActor.isRunning());

    flowActor.schedule(Action.FLOW_REFRESH, 10000);
    assertEquals(Set.of(Action.FLOW_REFRESH), flowActor.getScheduledActions().keySet());

    assertFalse(groupActor.checkShutdown());
    verify(future, times(0)).cancel(true);
    assertTrue(groupActor.isRunning());
  }

  @Test
  public void testCheckShutdownWithoutChildRunning() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    assertTrue(groupActor.isRunning());
    assertTrue(flowActor.isRunning());

    flowActor.schedule(Action.FLOW_REFRESH, 10000);
    assertEquals(Set.of(Action.FLOW_REFRESH), flowActor.getScheduledActions().keySet());

    assertTrue(flowActor.checkShutdown());
    verifyActions(groupActor, Action.FLOW_DOWN);
    assertFalse(flowActor.isRunning());

    assertTrue(groupActor.checkShutdown());
    verify(future, times(1)).cancel(true);
    assertFalse(groupActor.isRunning());
  }

  @Test
  public void testTerminateNow() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    assertTrue(groupActor.isRunning());
    groupActor.schedule(Action.GROUP_START, 10000);
    assertEquals(Set.of(Action.GROUP_START), groupActor.getScheduledActions().keySet());

    groupActor.terminateNow();
    assertFalse(groupActor.isRunning());
    verify(future, times(1)).cancel(true);
  }

  @Test
  public void testScheduleWithoutDelay() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.schedule(Action.GROUP_START, 0);
    verify(context, times(0)).schedule(any(), anyLong());
    verifyActions(groupActor, Action.GROUP_START);
  }

  @Test
  public void testScheduleWithoutDelayForDuplicateAction() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.schedule(Action.TASK_PING, 0);
    groupActor.schedule(Action.TASK_PING, 0);
    verify(context, times(0)).schedule(any(), anyLong());
    verifyActions(groupActor, Action.TASK_PING);
  }

  @Test
  public void testScheduleWithDelay() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.schedule(Action.GROUP_START, 10000);
    verify(context, times(1)).schedule(any(), anyLong());
    assertEquals(Set.of(Action.GROUP_START), groupActor.getScheduledActions().keySet());
    verifyEmptyAction(groupActor);
  }

  @Test
  public void testScheduleForNotRunningActor() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.terminateNow();

    groupActor.schedule(Action.GROUP_START, 10000);
    verify(context, times(0)).schedule(any(), anyLong());
    verifyEmptyAction(groupActor);
  }

  @Test
  public void testScheduleForDuplicatePendingAction() {
    var future = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future);

    groupActor.schedule(Action.GROUP_START, 10000);
    groupActor.schedule(Action.GROUP_START, 10000);
    verify(context, times(1)).schedule(any(), anyLong());
    assertEquals(Set.of(Action.GROUP_START), groupActor.getScheduledActions().keySet());
    verifyEmptyAction(groupActor);
  }

  @Test
  public void testScheduleForDuplicateDoneAction() {
    var future1 = Mockito.mock(ScheduledFuture.class);
    var future2 = Mockito.mock(ScheduledFuture.class);
    when(context.schedule(any(), anyLong())).thenReturn(future1).thenReturn(future2);
    when(future1.isDone()).thenReturn(true);

    groupActor.runActionFor(flowActor, Action.FLOW_START);
    groupActor.schedule(Action.GROUP_START, 10000);
    groupActor.startShutdown(Action.FLOW_SHUTDOWN);
    verify(future1, times(1)).cancel(true);
    assertEquals(Map.of(Action.GROUP_START, future1), groupActor.getScheduledActions());
    assertTrue(groupActor.isRunning());

    groupActor.schedule(Action.GROUP_START, 10000);
    verify(context, times(2)).schedule(any(), anyLong());
    verify(future1, times(1)).cancel(true);
    verify(future2, times(0)).cancel(true);
    assertEquals(Map.of(Action.GROUP_START, future2), groupActor.getScheduledActions());
  }

  @Test
  public void testRun() {
    flowActor.post(Action.TASK_DOWN);
    Map<Action, Boolean> queuedActions = flowActor.getQueuedActions();
    assertEquals(1, queuedActions.size());
    assertTrue(queuedActions.containsKey(Action.TASK_DOWN));
    flowActor.run();
    assertTrue(queuedActions.isEmpty());
    verify(context, times(2)).getMetrics();
    verify(metrics, times(1)).counter("num_of_running_flows", FlowActor.class);
    verify(metrics, times(1))
        .counter("num_of_finished_flows", FlowActor.class, "finalized", "false");
    verifyActions(groupActor, Action.FLOW_DOWN);
    verifyEmptyAction(flowActor);
  }
}
