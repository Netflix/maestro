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
package com.netflix.maestro.flow.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.flow.actor.Actor;
import com.netflix.maestro.flow.actor.ActorBaseTest;
import com.netflix.maestro.flow.models.Flow;
import com.netflix.maestro.flow.models.FlowDef;
import com.netflix.maestro.flow.models.FlowGroup;
import com.netflix.maestro.flow.models.MessagePayload;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FlowExecutorTest extends ActorBaseTest {

  private FlowExecutor executor;

  @Before
  public void setUp() {
    when(properties.getInitialMaintenanceDelayInMillis()).thenReturn(1L);
    when(properties.getMaintenanceDelayInMillis()).thenReturn(100000L);
    when(properties.getMaxGroupNumPerNode()).thenReturn(2L);
    when(properties.getExpirationDurationInMillis()).thenReturn(12000L);
    when(properties.getEngineAddress()).thenReturn("test-address");
    executor = new FlowExecutor(context);
  }

  @After
  public void tearDown() {
    executor.shutdown();
  }

  @Test
  public void testInitWithoutClaimingGroup() {
    var observer = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(observer);
    when(context.claimGroup()).thenReturn(null);
    executor.init();
    verify(context, timeout(10000)).claimGroup();
    executor.shutdown();
    verifyNoInteractions(observer);
  }

  @Test
  public void testInitWithClaimingGroup() {
    FlowGroup group = new FlowGroup(1, 1, "testAddress", 12345);
    when(context.claimGroup()).thenReturn(group);
    executor.init();
    verify(context, timeout(10000)).claimGroup();
    verify(context, timeout(3000).times(1)).run(any());
  }

  @Test
  public void testClaimingGroupNotifiesObservers() {
    FlowGroup group = new FlowGroup(1, 1, "testAddress", 12345);
    when(context.claimGroup()).thenReturn(group);
    List<Long> groupOwnershipUpdates = new CopyOnWriteArrayList<>();
    executor.registerAsObserver(groupOwnershipUpdates::add);
    executor.init();
    verify(context, timeout(10000)).claimGroup();
    // Wait for notification and actor creation, not just the start of the database claim.
    verify(context, timeout(3000)).run(any());
    assertEquals(1, groupOwnershipUpdates.size());
    assertEquals(Long.valueOf(1), groupOwnershipUpdates.getFirst());
  }

  @Test
  public void testMaintenanceWithTooManyGroups() {
    when(properties.getMaxGroupNumPerNode()).thenReturn(0L);
    executor = new FlowExecutor(context);
    var observer = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(observer);
    executor.init();
    verify(context, timeout(500).times(0)).claimGroup();
    verify(context, timeout(500).times(0)).run(any());
    verify(metrics, timeout(500).times(1)).gauge("num_of_groups", 0, FlowExecutor.class);
    executor.shutdown();
    verifyNoInteractions(observer);
  }

  @Test
  public void testClaimingGroupNotifiesMultipleObservers() {
    when(context.claimGroup()).thenReturn(new FlowGroup(7, 1, "test-address", 12345));
    var firstObserver = mock(GroupOwnershipChangeObserver.class);
    var secondObserver = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(firstObserver);
    executor.registerAsObserver(secondObserver);

    executor.init();

    verify(firstObserver, timeout(3000)).onGroupClaimed(7);
    verify(secondObserver, timeout(3000)).onGroupClaimed(7);
    verify(context, timeout(3000)).run(any());
    executor.shutdown();
    verifyNoMoreInteractions(firstObserver, secondObserver);
  }

  @Test
  public void testFailedClaimDoesNotNotifyObservers() {
    when(context.claimGroup()).thenThrow(new MaestroRetryableError("claim failed"));
    var observer = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(observer);

    executor.init();
    verify(context, timeout(3000)).claimGroup();
    executor.shutdown();

    verifyNoInteractions(observer);
    verify(context, never()).run(any());
  }

  @Test
  public void testShutdown() {
    executor.shutdown();
    verify(context, times(1)).shutdown();
  }

  @Test
  public void testStartFlow() {
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));
    var id = executor.startFlow(1, "test-id", "wf-1", new FlowDef(), Map.of());
    verify(context, times(1)).trySaveGroup(anyLong(), any());
    var actorCaptor = ArgumentCaptor.forClass(Actor.class);
    verify(context, times(1)).run(actorCaptor.capture());
    var flowCaptor = ArgumentCaptor.forClass(Flow.class);
    verify(context, times(1)).saveFlow(flowCaptor.capture());
    assertEquals("test-id", id);
    assertTrue(actorCaptor.getValue().isRunning());
    assertEquals("wf-1", flowCaptor.getValue().getReference());
  }

  @Test
  public void testStartFlowNotifiesObserversOfGroup() {
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));
    List<Long> groupOwnershipUpdates = new ArrayList<>();
    executor.registerAsObserver(groupOwnershipUpdates::add);

    var id = executor.startFlow(1, "test-id", "wf-1", new FlowDef(), Map.of());
    verify(context, times(1)).trySaveGroup(anyLong(), any());
    var actorCaptor = ArgumentCaptor.forClass(Actor.class);
    verify(context, times(1)).run(actorCaptor.capture());
    var flowCaptor = ArgumentCaptor.forClass(Flow.class);
    verify(context, times(1)).saveFlow(flowCaptor.capture());

    assertEquals(1, groupOwnershipUpdates.size());
    assertEquals(Long.valueOf(1), groupOwnershipUpdates.getFirst());
  }

  @Test
  public void testStartFlowNotifiesMultipleObserversForEachNewGroup() {
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));
    when(context.trySaveGroup(2, "test-address"))
        .thenReturn(new FlowGroup(2, 1, "test-address", 12345));
    var firstObserver = mock(GroupOwnershipChangeObserver.class);
    var secondObserver = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(firstObserver);
    executor.registerAsObserver(secondObserver);

    executor.startFlow(1, "test-id-1", "wf-1", new FlowDef(), Map.of());
    executor.startFlow(2, "test-id-2", "wf-2", new FlowDef(), Map.of());

    verify(firstObserver).onGroupClaimed(1);
    verify(firstObserver).onGroupClaimed(2);
    verify(secondObserver).onGroupClaimed(1);
    verify(secondObserver).onGroupClaimed(2);
    verifyNoMoreInteractions(firstObserver, secondObserver);
  }

  @Test
  public void testStartFlowInExistingGroupDoesNotNotifyObserversAgain() {
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));
    var observer = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(observer);

    executor.startFlow(1, "test-id-1", "wf-1", new FlowDef(), Map.of());
    executor.startFlow(1, "test-id-2", "wf-2", new FlowDef(), Map.of());

    verify(observer).onGroupClaimed(1);
    verifyNoMoreInteractions(observer);
    verify(context).trySaveGroup(1, "test-address");
    verify(context, times(2)).saveFlow(any());
  }

  @Test
  public void testFailedGroupCreationNotifiesObserversOnlyOnSuccessfulRetry() {
    when(context.trySaveGroup(1, "test-address"))
        .thenThrow(new MaestroRetryableError("group already owned"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));
    var observer = mock(GroupOwnershipChangeObserver.class);
    executor.registerAsObserver(observer);

    assertThrows(
        MaestroRetryableError.class,
        () -> executor.startFlow(1, "test-id", "wf-1", new FlowDef(), Map.of()));
    verifyNoInteractions(observer);
    verify(context, never()).run(any());
    verify(context, never()).saveFlow(any());

    assertEquals("test-id", executor.startFlow(1, "test-id", "wf-1", new FlowDef(), Map.of()));
    verify(observer).onGroupClaimed(1);
    verifyNoMoreInteractions(observer);
    verify(context, times(2)).trySaveGroup(1, "test-address");
    verify(context).saveFlow(any());
  }

  @Test
  public void testObserverRegisteredDuringNotificationReceivesSubsequentClaims() {
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));
    when(context.trySaveGroup(2, "test-address"))
        .thenReturn(new FlowGroup(2, 1, "test-address", 12345));
    var observer = mock(GroupOwnershipChangeObserver.class);
    List<Long> groupOwnershipUpdates = new ArrayList<>();
    executor.registerAsObserver(
        groupId -> {
          groupOwnershipUpdates.add(groupId);
          if (groupId == 1) {
            executor.registerAsObserver(observer);
          }
        });

    executor.startFlow(1, "test-id-1", "wf-1", new FlowDef(), Map.of());
    verifyNoInteractions(observer);
    executor.startFlow(2, "test-id-2", "wf-2", new FlowDef(), Map.of());

    assertEquals(List.of(1L, 2L), groupOwnershipUpdates);
    verify(observer).onGroupClaimed(2);
    verifyNoMoreInteractions(observer);
  }

  @Test
  public void testWakeUp() {
    assertFalse(executor.wakeUp(1L, "wf-1", "task1", 0));
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));

    executor.startFlow(1, "test-id", "wf-1", new FlowDef(), Map.of());
    assertTrue(executor.wakeUp(1L, "wf-1", "task1", 0));
    assertFalse(executor.wakeUp(2L, "wf-1", "task1", 0));
    assertTrue(executor.wakeUp(1L, "wf-2", "task1", 0));

    // Test with custom action code
    assertTrue(executor.wakeUp(1L, "wf-1", "task1", 123));
    assertFalse(executor.wakeUp(2L, "wf-1", "task1", -1));
    assertTrue(executor.wakeUp(1L, "wf-2", null, 123));
  }

  @Test
  public void testWakeUpWithPayload() {
    assertFalse(executor.wakeUp(1L, "wf-1", "task1", 0, MessagePayload.DEFAULT));
    when(context.trySaveGroup(1, "test-address"))
        .thenReturn(new FlowGroup(1, 1, "test-address", 12345));

    executor.startFlow(1, "test-id", "wf-1", new FlowDef(), Map.of());
    assertTrue(executor.wakeUp(1L, "wf-1", "task1", 0, MessagePayload.DEFAULT));
    assertFalse(executor.wakeUp(2L, "wf-1", "task1", 0, MessagePayload.DEFAULT));
    assertTrue(executor.wakeUp(1L, "wf-2", "task1", 123, MessagePayload.DEFAULT));
  }
}
