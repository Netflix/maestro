package com.netflix.maestro.signal.producer;

import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Queue producer for signal related messages.
 *
 * @author jun-he
 */
public interface SignalQueueProducer {
  /**
   * Push a signal instance asynchronously (non-transactional).
   *
   * @param signalInstance the signal instance to push
   */
  void push(SignalInstance signalInstance);

  /**
   * Push a signal trigger match asynchronously (non-transactional).
   *
   * @param triggerMatch the signal trigger match to push
   */
  void push(SignalTriggerMatch triggerMatch);

  /**
   * Push a signal trigger execution asynchronously (non-transactional).
   *
   * @param triggerExecution the signal trigger execution to push
   */
  void push(SignalTriggerExecution triggerExecution);

  /**
   * Push a signal instance in the same database transaction (transactional).
   *
   * @param conn the database connection for the transaction
   * @param signalInstance the signal instance to push
   * @throws SQLException if the database operation fails
   */
  void pushInTransaction(Connection conn, SignalInstance signalInstance) throws SQLException;

  /**
   * Push a signal trigger match in the same database transaction (transactional).
   *
   * @param conn the database connection for the transaction
   * @param triggerMatch the signal trigger match to push
   * @throws SQLException if the database operation fails
   */
  void pushInTransaction(Connection conn, SignalTriggerMatch triggerMatch) throws SQLException;

  /**
   * Push a signal trigger execution in the same database transaction (transactional).
   *
   * @param conn the database connection for the transaction
   * @param triggerExecution the signal trigger execution to push
   * @throws SQLException if the database operation fails
   */
  void pushInTransaction(Connection conn, SignalTriggerExecution triggerExecution)
      throws SQLException;
}
