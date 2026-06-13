/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.signal.producer;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalTriggerExecution;
import com.netflix.maestro.models.signal.SignalTriggerMatch;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.queue.models.MessageDto;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

/**
 * Unit tests for SignalQueueProducer implementation that uses MaestroQueueSystem.
 *
 * @author test
 */
public class SignalQueueProducerTest extends MaestroBaseTest {
  @Mock private MaestroQueueSystem queueSystem;
  @Mock private Connection connection;
  @Mock private MessageDto messageDto;

  private SignalQueueProducer signalQueueProducer;

  @Before
  public void setUp() {
    // Instantiate the producer implementation that will be created
    signalQueueProducer = new MaestroSignalQueueProducerImpl(queueSystem);
  }

  /**
   * Test pushing a SignalInstance without connection (async, non-transactional). Should enqueue via
   * MaestroQueueSystem.enqueueOrThrow(jobEvent).
   */
  @Test
  public void testPushSignalInstanceAsync() {
    // Arrange
    SignalInstance signalInstance = createTestSignalInstance("signal-1", "name1");

    // Act
    signalQueueProducer.push(signalInstance);

    // Assert
    verify(queueSystem, times(1)).enqueueOrThrow(any(MaestroJobEvent.class));

    // Verify the job event passed is of type SIGNAL_INSTANCE
    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem).enqueueOrThrow(eventCaptor.capture());
    MaestroJobEvent capturedEvent = eventCaptor.getValue();
    assertEquals(MaestroJobEvent.Type.SIGNAL_INSTANCE, capturedEvent.getType());
  }

  /**
   * Test pushing a SignalTriggerMatch without connection (async, non-transactional). Should enqueue
   * via MaestroQueueSystem.enqueueOrThrow(jobEvent).
   */
  @Test
  public void testPushSignalTriggerMatchAsync() {
    // Arrange
    SignalTriggerMatch triggerMatch = createTestSignalTriggerMatch("workflow-1", "trigger-uuid-1");

    // Act
    signalQueueProducer.push(triggerMatch);

    // Assert
    verify(queueSystem, times(1)).enqueueOrThrow(any(MaestroJobEvent.class));

    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem).enqueueOrThrow(eventCaptor.capture());
    MaestroJobEvent capturedEvent = eventCaptor.getValue();
    assertEquals(MaestroJobEvent.Type.SIGNAL_TRIGGER_MATCH, capturedEvent.getType());
  }

  /**
   * Test pushing a SignalTriggerExecution without connection (async, non-transactional). Should
   * enqueue via MaestroQueueSystem.enqueueOrThrow(jobEvent).
   */
  @Test
  public void testPushSignalTriggerExecutionAsync() {
    // Arrange
    SignalTriggerExecution triggerExecution =
        createTestSignalTriggerExecution("workflow-2", "trigger-uuid-2");

    // Act
    signalQueueProducer.push(triggerExecution);

    // Assert
    verify(queueSystem, times(1)).enqueueOrThrow(any(MaestroJobEvent.class));

    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem).enqueueOrThrow(eventCaptor.capture());
    MaestroJobEvent capturedEvent = eventCaptor.getValue();
    assertEquals(MaestroJobEvent.Type.SIGNAL_TRIGGER_EXECUTION, capturedEvent.getType());
  }

  /** Test that async push succeeds (no exception thrown). */
  @Test
  public void testPushSignalInstanceAsyncSuccess() {
    // Arrange
    SignalInstance signalInstance = createTestSignalInstance("signal-2", "name2");

    // Act & Assert (should not throw)
    signalQueueProducer.push(signalInstance);

    verify(queueSystem, times(1)).enqueueOrThrow(any(MaestroJobEvent.class));
  }

  /**
   * Test that async push throws exception if enqueueOrThrow fails. This tests the error handling
   * path.
   */
  @Test
  public void testPushSignalInstanceAsyncThrowsOnError() {
    // Arrange
    SignalInstance signalInstance = createTestSignalInstance("signal-3", "name3");
    RuntimeException testException = new RuntimeException("Queue system error");

    doThrow(testException).when(queueSystem).enqueueOrThrow(any(MaestroJobEvent.class));

    // Act & Assert
    AssertHelper.assertThrows(
        "Expected RuntimeException to be thrown",
        RuntimeException.class,
        "Queue system error",
        () -> signalQueueProducer.push(signalInstance));
  }

  /**
   * Test transactional push of SignalInstance with connection. Should enqueue via
   * MaestroQueueSystem.enqueue(Connection, jobEvent).
   */
  @Test
  public void testPushSignalInstanceTransactional() throws Exception {
    // Arrange
    SignalInstance signalInstance = createTestSignalInstance("signal-4", "name4");
    when(queueSystem.enqueue(any(Connection.class), any(MaestroJobEvent.class)))
        .thenReturn(messageDto);

    // Act
    signalQueueProducer.pushInTransaction(connection, signalInstance);

    // Assert
    verify(queueSystem, times(1)).enqueue(any(Connection.class), any(MaestroJobEvent.class));

    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem).enqueue(any(Connection.class), eventCaptor.capture());
    MaestroJobEvent capturedEvent = eventCaptor.getValue();
    assertEquals(MaestroJobEvent.Type.SIGNAL_INSTANCE, capturedEvent.getType());
  }

  /** Test transactional push of SignalTriggerMatch with connection. */
  @Test
  public void testPushSignalTriggerMatchTransactional() throws Exception {
    // Arrange
    SignalTriggerMatch triggerMatch = createTestSignalTriggerMatch("workflow-3", "trigger-uuid-3");
    when(queueSystem.enqueue(any(Connection.class), any(MaestroJobEvent.class)))
        .thenReturn(messageDto);

    // Act
    signalQueueProducer.pushInTransaction(connection, triggerMatch);

    // Assert
    verify(queueSystem, times(1)).enqueue(any(Connection.class), any(MaestroJobEvent.class));

    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem).enqueue(any(Connection.class), eventCaptor.capture());
    MaestroJobEvent capturedEvent = eventCaptor.getValue();
    assertEquals(MaestroJobEvent.Type.SIGNAL_TRIGGER_MATCH, capturedEvent.getType());
  }

  /** Test transactional push of SignalTriggerExecution with connection. */
  @Test
  public void testPushSignalTriggerExecutionTransactional() throws Exception {
    // Arrange
    SignalTriggerExecution triggerExecution =
        createTestSignalTriggerExecution("workflow-4", "trigger-uuid-4");
    when(queueSystem.enqueue(any(Connection.class), any(MaestroJobEvent.class)))
        .thenReturn(messageDto);

    // Act
    signalQueueProducer.pushInTransaction(connection, triggerExecution);

    // Assert
    verify(queueSystem, times(1)).enqueue(any(Connection.class), any(MaestroJobEvent.class));

    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem).enqueue(any(Connection.class), eventCaptor.capture());
    MaestroJobEvent capturedEvent = eventCaptor.getValue();
    assertEquals(MaestroJobEvent.Type.SIGNAL_TRIGGER_EXECUTION, capturedEvent.getType());
  }

  /**
   * Test that transactional push throws exception if pushInTransaction fails. This tests the error
   * handling path for transactional pushes.
   */
  @Test
  public void testPushSignalInstanceTransactionalThrowsOnError() throws Exception {
    // Arrange
    SignalInstance signalInstance = createTestSignalInstance("signal-4", "name4");
    RuntimeException testException = new RuntimeException("Transaction error");

    // Mock call throws error
    doThrow(testException)
        .when(queueSystem)
        .enqueue(any(Connection.class), any(MaestroJobEvent.class));

    // Act & Assert
    AssertHelper.assertThrows(
        "Expected RuntimeException to be thrown",
        RuntimeException.class,
        "Transaction error",
        () -> {
          signalQueueProducer.pushInTransaction(connection, signalInstance);
          return null;
        });
  }

  /** Test that all three types of signals can be pushed in sequence. */
  @Test
  public void testMultiplePushOperations() {
    // Arrange
    SignalInstance signalInstance = createTestSignalInstance("signal-5", "name5");
    SignalTriggerMatch triggerMatch = createTestSignalTriggerMatch("workflow-5", "trigger-uuid-5");
    SignalTriggerExecution triggerExecution =
        createTestSignalTriggerExecution("workflow-6", "trigger-uuid-6");

    // Act
    signalQueueProducer.push(signalInstance);
    signalQueueProducer.push(triggerMatch);
    signalQueueProducer.push(triggerExecution);

    // Assert - verify all three were enqueued
    verify(queueSystem, times(3)).enqueueOrThrow(any(MaestroJobEvent.class));

    ArgumentCaptor<MaestroJobEvent> eventCaptor = ArgumentCaptor.forClass(MaestroJobEvent.class);
    verify(queueSystem, times(3)).enqueueOrThrow(eventCaptor.capture());
    List<MaestroJobEvent> capturedEvents = eventCaptor.getAllValues();
    assertEquals(MaestroJobEvent.Type.SIGNAL_INSTANCE, capturedEvents.get(0).getType());
    assertEquals(MaestroJobEvent.Type.SIGNAL_TRIGGER_MATCH, capturedEvents.get(1).getType());
    assertEquals(MaestroJobEvent.Type.SIGNAL_TRIGGER_EXECUTION, capturedEvents.get(2).getType());
  }

  // ============= Helper methods to create test data =============

  private SignalInstance createTestSignalInstance(String instanceId, String name) {
    SignalInstance instance = new SignalInstance();
    instance.setInstanceId(instanceId);
    instance.setName(name);
    instance.setSeqId(1L);
    instance.setCreateTime(System.currentTimeMillis());
    return instance;
  }

  private SignalTriggerMatch createTestSignalTriggerMatch(String workflowId, String triggerUuid) {
    SignalTriggerMatch match = new SignalTriggerMatch();
    match.setWorkflowId(workflowId);
    match.setTriggerUuid(triggerUuid);
    SignalInstance signalInstance = createTestSignalInstance("signal-match-1", "signal-name-1");
    match.setSignalInstance(signalInstance);
    return match;
  }

  private SignalTriggerExecution createTestSignalTriggerExecution(
      String workflowId, String triggerUuid) {
    SignalTriggerExecution execution = new SignalTriggerExecution();
    execution.setWorkflowId(workflowId);
    execution.setTriggerUuid(triggerUuid);
    execution.setSignalIds(new HashMap<>(Map.of("signal-1", 1L)));
    execution.setCondition("true");
    return execution;
  }
}
