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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.flow.models.Flow;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

public class GroupActorTest extends ActorBaseTest {
  private GroupActor groupActor;
  private Flow flow;

  @Before
  public void init() {
    groupActor = createGroupActor();
    flow = createFlow();
  }

  @Test
  public void testGeneration() {
    assertEquals(2, groupActor.generation());
  }

  @Test
  public void testBeforeRunning() {
    groupActor.beforeRunning();
    verify(context, times(1)).schedule(any(), anyLong());
    assertEquals(Set.of(Action.GROUP_HEARTBEAT), groupActor.getScheduledActions().keySet());
    verify(metrics, times(1)).counter("num_of_running_groups", GroupActor.class);
  }

  @Test
  public void testRunForActionStartGroupWithAnException() {
    when(context.getFlowsFrom(any(), anyLong(), any())).thenReturn(null);
    groupActor.runForAction(Action.GROUP_START);

    verify(context, times(1)).getFlowsFrom(any(), anyLong(), any());
    verify(context, times(1)).schedule(any(), anyLong());
    assertEquals(Set.of(Action.GROUP_START), groupActor.getScheduledActions().keySet());
  }

  @Test
  public void testRunForActionStartGroupWithoutNewFlow() {
    when(context.getFlowsFrom(any(), anyLong(), any())).thenReturn(List.of());
    groupActor.runForAction(Action.GROUP_START);

    verify(context, times(1)).getFlowsFrom(any(), anyLong(), any());
    verify(context, times(0)).schedule(any(), anyLong());
    assertTrue(groupActor.getScheduledActions().isEmpty());
  }

  @Test
  public void testRunForActionStartGroupWithNewFlow() {
    when(context.getFlowsFrom(any(), anyLong(), any()))
        .thenReturn(List.of(flow))
        .thenReturn(List.of());
    groupActor.runForAction(Action.GROUP_START);

    verifyActions(groupActor, new Action.FlowLaunch(flow, true));
    verify(context, times(2)).getFlowsFrom(any(), anyLong(), any());
    verify(context, times(0)).schedule(any(), anyLong());
    assertTrue(groupActor.getScheduledActions().isEmpty());
  }

  @Test
  public void testRunForActionFlowLaunchResume() {
    groupActor.runForAction(new Action.FlowLaunch(flow, true));

    verify(context, times(1)).run(any());
    var child = groupActor.getChild(flow.getReference());
    verifyActions(child, Action.FLOW_RESUME);

    groupActor.runForAction(new Action.FlowLaunch(flow, true));
    verify(context, times(1)).run(any());
    assertEquals(child, groupActor.getChild(flow.getReference()));
  }

  @Test
  public void testRunForActionFlowLaunchStart() {
    groupActor.runForAction(new Action.FlowLaunch(flow, false));

    verify(context, times(1)).run(any());
    var child = groupActor.getChild(flow.getReference());
    verifyActions(child, Action.FLOW_START);

    groupActor.runForAction(new Action.FlowLaunch(flow, true));
    verify(context, times(1)).run(any());
    assertEquals(child, groupActor.getChild(flow.getReference()));
  }

  @Test
  public void testRunForActionFlowWakeUp() {
    groupActor.runForAction(new Action.FlowWakeUp(flow.getReference(), "taskRef"));
    assertNull(groupActor.getChild(flow.getReference()));

    groupActor.runForAction(new Action.FlowLaunch(flow, false));
    verify(context, times(1)).run(any());
    var child = groupActor.getChild(flow.getReference());
    verifyActions(child, Action.FLOW_START);

    groupActor.runForAction(new Action.FlowWakeUp(flow.getReference(), "taskRef"));
    verifyActions(child, new Action.TaskWakeUp("taskRef"));
  }

  @Test
  public void testGroupHeartbeat() {
    groupActor.runForAction(Action.GROUP_HEARTBEAT);

    verify(context, times(1)).heartbeatGroup(any());
    verify(context, times(1)).schedule(any(), anyLong());
    assertEquals(Set.of(Action.GROUP_HEARTBEAT), groupActor.getScheduledActions().keySet());
    verify(metrics, times(1))
        .gauge("current_running_groups", 1.0, GroupActor.class, "group", "group-1");
  }

  @Test
  public void testGroupShutdown() {
    assertTrue(groupActor.isRunning());

    groupActor.runForAction(Action.GROUP_SHUTDOWN);
    assertFalse(groupActor.isRunning());
    verifyEmptyAction(groupActor);
    assertTrue(groupActor.getScheduledActions().isEmpty());
  }

  @Test
  public void testFlowDown() {
    assertTrue(groupActor.isRunning());

    groupActor.runForAction(Action.FLOW_DOWN);
    assertFalse(groupActor.isRunning());
    verifyEmptyAction(groupActor);
    assertTrue(groupActor.getScheduledActions().isEmpty());
  }

  @Test
  public void testUnexpectedAction() {
    AssertHelper.assertThrows(
        "should throw for unexpected action",
        MaestroUnprocessableEntityException.class,
        "Unexpected action: [TaskDown[]] for FlowGroup [group-1]",
        () -> groupActor.runForAction(Action.TASK_DOWN));
  }

  @Test
  public void testAfterRunning() {
    groupActor.afterRunning();
    verify(context, times(1)).releaseGroup(any());
    verify(metrics, times(1)).counter("num_of_finished_groups", GroupActor.class);
  }

  @Test
  public void testReference() {
    assertEquals("group-1", groupActor.reference());
  }
}
