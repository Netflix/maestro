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

import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalTriggerExecution;
import com.netflix.maestro.models.signal.SignalTriggerMatch;
import com.netflix.maestro.queue.MaestroQueueSystem;
import com.netflix.maestro.queue.jobevents.SignalInstanceJobEvent;
import com.netflix.maestro.queue.jobevents.SignalTriggerExecutionJobEvent;
import com.netflix.maestro.queue.jobevents.SignalTriggerMatchJobEvent;
import java.sql.Connection;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of SignalQueueProducer using MaestroQueueSystem. This implementation provides
 * exactly-once semantics for signal processing through transactional queue operations.
 *
 * @author maestro
 */
@Slf4j
@AllArgsConstructor
public class MaestroSignalQueueProducerImpl implements SignalQueueProducer {
  private final MaestroQueueSystem queueSystem;

  /**
   * Push a signal instance asynchronously (non-transactional). The signal instance is wrapped in a
   * SignalInstanceJobEvent and enqueued via MaestroQueueSystem.
   *
   * @param signalInstance the signal instance to push
   */
  @Override
  public void push(SignalInstance signalInstance) {
    LOG.debug("Pushing signal instance [{}] asynchronously", signalInstance.getInstanceId());
    var jobEvent = SignalInstanceJobEvent.create(signalInstance);
    queueSystem.enqueueOrThrow(jobEvent);
  }

  /**
   * Push a signal trigger match asynchronously (non-transactional). The signal trigger match is
   * wrapped in a SignalTriggerMatchJobEvent and enqueued via MaestroQueueSystem.
   *
   * @param triggerMatch the signal trigger match to push
   */
  @Override
  public void push(SignalTriggerMatch triggerMatch) {
    LOG.debug(
        "Pushing signal trigger match [workflow: {}, trigger: {}] asynchronously",
        triggerMatch.getWorkflowId(),
        triggerMatch.getTriggerUuid());
    var jobEvent = SignalTriggerMatchJobEvent.create(triggerMatch);
    queueSystem.enqueueOrThrow(jobEvent);
  }

  /**
   * Push a signal trigger execution asynchronously (non-transactional). The signal trigger
   * execution is wrapped in a SignalTriggerExecutionJobEvent and enqueued via MaestroQueueSystem.
   *
   * @param triggerExecution the signal trigger execution to push
   */
  @Override
  public void push(SignalTriggerExecution triggerExecution) {
    LOG.debug(
        "Pushing signal trigger execution [workflow: {}, trigger: {}] asynchronously",
        triggerExecution.getWorkflowId(),
        triggerExecution.getTriggerUuid());
    var jobEvent = SignalTriggerExecutionJobEvent.create(triggerExecution);
    queueSystem.enqueueOrThrow(jobEvent);
  }

  /**
   * Push a signal instance within a database transaction. This ensures exactly-once semantics by
   * atomically committing both the database write and queue message in the same transaction.
   *
   * @param conn the database connection for the transaction
   * @param signalInstance the signal instance to push
   * @throws SQLException if the database operation fails
   */
  @Override
  public void pushInTransaction(Connection conn, SignalInstance signalInstance)
      throws SQLException {
    LOG.debug("Pushing signal instance [{}] in transaction", signalInstance.getInstanceId());
    var jobEvent = SignalInstanceJobEvent.create(signalInstance);
    queueSystem.enqueue(conn, jobEvent);
  }

  /**
   * Push a signal trigger match within a database transaction. This ensures exactly-once semantics
   * by atomically committing both the database write and queue message in the same transaction.
   *
   * @param conn the database connection for the transaction
   * @param triggerMatch the signal trigger match to push
   * @throws SQLException if the database operation fails
   */
  @Override
  public void pushInTransaction(Connection conn, SignalTriggerMatch triggerMatch)
      throws SQLException {
    LOG.debug(
        "Pushing signal trigger match [workflow: {}, trigger: {}] in transaction",
        triggerMatch.getWorkflowId(),
        triggerMatch.getTriggerUuid());
    var jobEvent = SignalTriggerMatchJobEvent.create(triggerMatch);
    queueSystem.enqueue(conn, jobEvent);
  }

  /**
   * Push a signal trigger execution within a database transaction. This ensures exactly-once
   * semantics by atomically committing both the database write and queue message in the same
   * transaction.
   *
   * @param conn the database connection for the transaction
   * @param triggerExecution the signal trigger execution to push
   * @throws SQLException if the database operation fails
   */
  @Override
  public void pushInTransaction(Connection conn, SignalTriggerExecution triggerExecution)
      throws SQLException {
    LOG.debug(
        "Pushing signal trigger execution [workflow: {}, trigger: {}] in transaction",
        triggerExecution.getWorkflowId(),
        triggerExecution.getTriggerUuid());
    var jobEvent = SignalTriggerExecutionJobEvent.create(triggerExecution);
    queueSystem.enqueue(conn, jobEvent);
  }
}
