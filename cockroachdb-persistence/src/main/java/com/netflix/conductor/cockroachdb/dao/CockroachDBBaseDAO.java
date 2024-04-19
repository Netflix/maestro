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
package com.netflix.conductor.cockroachdb.dao;

import static com.netflix.conductor.core.execution.ApplicationException.Code.BACKEND_ERROR;
import static com.netflix.conductor.core.execution.ApplicationException.Code.INTERNAL_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.cockroachdb.util.ResultProcessor;
import com.netflix.conductor.cockroachdb.util.StatementFunction;
import com.netflix.conductor.cockroachdb.util.StatementPreparer;
import com.netflix.conductor.cockroachdb.util.TransactionalFunction;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.metrics.Monitors;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CockroachDB Base DAO to support basic DB operation.
 *
 * @author jun-he
 */
public abstract class CockroachDBBaseDAO {
  private static final Logger LOG = LoggerFactory.getLogger(CockroachDBBaseDAO.class);

  private static final String SAVEPOINT_NAME = "cockroach_restart";
  private static final String RETRY_SQL_STATE = "40001";

  private static final String VERSION_COLUMN = "version";
  protected static final String PAYLOAD_COLUMN = "payload";
  protected static final String ID_COLUMN = "id";
  protected static final String STATUS_COLUMN = "status";
  protected static final int SUCCESS_WRITE_SIZE = 1;
  protected static final String ARRAY_TYPE_NAME = "TEXT";
  static final String SET_FOLLOWER_READS_MODE =
      "SET TRANSACTION AS OF SYSTEM TIME experimental_follower_read_timestamp()";

  private final ObjectMapper objectMapper;
  private final DataSource dataSource;
  private final int maxRetries;
  private final boolean isFollowerReadsEnabled;
  private final int maxRetryDelay;
  private final int initialRetryDelay;

  public CockroachDBBaseDAO(
      DataSource dataSource, ObjectMapper objectMapper, CockroachDBConfiguration config) {
    this.dataSource = dataSource;
    this.objectMapper = objectMapper;
    this.maxRetries = config.getDbErrorRetries();
    this.isFollowerReadsEnabled = config.isFollowerReadsEnabled();
    this.maxRetryDelay = config.getDbMaxRetryDelay();
    this.initialRetryDelay = config.getDbInitialRetryDelay();
  }

  @VisibleForTesting
  void validateTasks(Collection<Task> tasks) {
    Preconditions.checkNotNull(tasks, "Tasks object cannot be null");
    Preconditions.checkArgument(!tasks.isEmpty(), "Tasks object cannot be empty");
    tasks.forEach(
        task -> {
          Preconditions.checkNotNull(task, "task object cannot be null");
          Preconditions.checkNotNull(task.getTaskId(), "Task id cannot be null");
          Preconditions.checkNotNull(
              task.getWorkflowInstanceId(), "Workflow instance id cannot be null");
          Preconditions.checkNotNull(
              task.getReferenceTaskName(), "Task reference name cannot be null");
        });

    Preconditions.checkArgument(
        tasks.stream().map(Task::getWorkflowInstanceId).distinct().count() <= 1,
        "Tasks of multiple workflows cannot be created/updated simultaneously");
  }

  @VisibleForTesting
  void validateWorkflow(Workflow workflow) {
    Preconditions.checkNotNull(workflow.getWorkflowId());
  }

  <T> T getPayload(String stmt, StatementPreparer preparer, Class<T> clazz) {
    return withRetryableQuery(stmt, preparer, r -> payloadFromResult(r, clazz));
  }

  <T> T getReadOnlyPayload(String stmt, StatementPreparer preparer, Class<T> clazz) {
    return withReadOnlyQuery(stmt, preparer, r -> payloadFromResult(r, clazz));
  }

  protected <T> List<T> getPayloads(String stmt, StatementPreparer preparer, Class<T> clazz) {
    return withRetryableQuery(stmt, preparer, r -> payloadsFromResult(r, clazz));
  }

  List<String> idsFromResult(ResultSet result, int version) throws SQLException {
    List<String> ids = new ArrayList<>();
    while (result.next()) {
      if (version < 0 || version == result.getInt(VERSION_COLUMN)) {
        ids.add(result.getString(ID_COLUMN));
      }
    }
    return ids;
  }

  private <T> T payloadFromResult(ResultSet result, Class<T> clazz) throws SQLException {
    if (result.next()) {
      String payload = result.getString(PAYLOAD_COLUMN);
      if (payload != null && !payload.isEmpty()) {
        return fromJson(payload, clazz);
      }
    }
    return null;
  }

  private <T> List<T> payloadsFromResult(ResultSet result, Class<T> clazz) throws SQLException {
    List<T> results = new ArrayList<>();
    while (result.next()) {
      String payload = result.getString(PAYLOAD_COLUMN);
      if (payload != null && !payload.isEmpty()) {
        results.add(fromJson(payload, clazz));
      }
    }
    return results;
  }

  /**
   * Convert the object to the JSON string
   *
   * @param value the object value
   * @return serialized string
   */
  protected String toJson(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new ApplicationException(INTERNAL_ERROR, e);
    }
  }

  /**
   * Parse a JSON to a given class instance
   *
   * @param json json string
   * @return deserialized object
   */
  protected <T> T fromJson(String json, Class<T> clazz) {
    try {
      return objectMapper.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new ApplicationException(INTERNAL_ERROR, e);
    }
  }

  /**
   * Parse a JSON to a given TypeReference instance
   *
   * @param json json string
   * @return deserialized object
   */
  protected <T> T fromJson(String json, TypeReference<T> valueTypeRef) {
    try {
      return objectMapper.readValue(json, valueTypeRef);
    } catch (JsonProcessingException e) {
      throw new ApplicationException(INTERNAL_ERROR, e);
    }
  }

  /**
   * A wrapper class to wrap the common logic for try ... catch...
   *
   * @param supplier a provided wrapped function, it may throw runtime exceptions
   * @param methodName a provided method name to log metrics
   * @param log a provided log string with argument support
   * @param args an array of arguments for logging
   * @return the result from supplier
   */
  protected <T> T withMetricLogError(
      Supplier<T> supplier, String methodName, String log, Object... args) {
    try {
      return supplier.get();
    } catch (Exception e) {
      Object[] combinedArgs;
      if (args != null) {
        combinedArgs = new Object[args.length + 1];
        System.arraycopy(args, 0, combinedArgs, 0, args.length);
      } else {
        combinedArgs = new Object[1];
      }
      combinedArgs[combinedArgs.length - 1] = e.getMessage();
      Monitors.error(this.getClass().getName(), methodName);
      LOG.error(log + " due to {}", combinedArgs);
      throw e;
    }
  }

  /**
   * Initialize a new transactional {@link Connection} PreparedStatement {@link PreparedStatement}
   * from {@link #dataSource} and pass it to {@literal statement preparer}. After querying the
   * results from DB, it then passes them to a processor. It uses follower read mode if supported
   * and does not retry.
   *
   * @param statement sql statement
   * @param preparer function to set parameters of the prepared statement
   * @param processor processor to process the result
   * @return the result
   * @throws ApplicationException If any errors occur.
   */
  protected <T> T withReadOnlyQuery(
      final String statement, StatementPreparer preparer, ResultProcessor<T> processor) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(statement)) {
      if (isFollowerReadsEnabled) {
        try (PreparedStatement stmt1 = conn.prepareStatement(SET_FOLLOWER_READS_MODE)) {
          stmt1.executeUpdate();
        }
      }
      preparer.prepare(stmt);
      try (ResultSet result = stmt.executeQuery()) {
        return processor.process(result);
      }
    } catch (SQLException e) {
      LOG.error(
          "with readOnlyQuery, non-retryable exception occurred: sql state = [{}], message = [{}]",
          e.getSQLState(),
          e.getMessage());
      throw new ApplicationException(BACKEND_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Initialize a new transactional {@link Connection} PreparedStatement {@link PreparedStatement}
   * from {@link #dataSource} and pass it to {@literal statement preparer}. After querying the
   * results from DB, it then passes them to a processor. It includes a retry mechanism as
   * CockroachDB recommend to implement client side retry
   *
   * @param stmt sql statement
   * @param preparer function to set parameters of the prepared statement
   * @param processor processor to process the result
   * @return result for the query
   */
  protected <T> T withRetryableQuery(
      String stmt, StatementPreparer preparer, ResultProcessor<T> processor) {
    return withRetryableStatement(
        stmt,
        statement -> {
          preparer.prepare(statement);
          try (ResultSet result = statement.executeQuery()) {
            return processor.process(result);
          }
        });
  }

  /**
   * Initialize a new transactional {@link Connection} PreparedStatement {@link PreparedStatement}
   * from {@link #dataSource} and pass it to {@literal statement preparer}. Then it updates the DB
   * records accordingly with retry support.
   *
   * @param stmt sql statement
   * @param preparer function to set parameters of the prepared statement
   * @return result for the update
   */
  protected int withRetryableUpdate(String stmt, StatementPreparer preparer) {
    return withRetryableStatement(
        stmt,
        statement -> {
          preparer.prepare(statement);
          return statement.executeUpdate();
        });
  }

  /**
   * Initialize a new transactional {@link Connection} from {@link #dataSource} and pass it to
   * {@literal function}. It includes a retry mechanism as CockroachDB recommend to implement client
   * side retry
   *
   * <p>Successful executions of {@literal function} will result in a commit and return of {@link
   * TransactionalFunction#apply)}.
   *
   * <p>If SQL exception has a retry sql state without exhausting the retry chances, the transaction
   * rollback to the save point and retry. Otherwise, the error will be thrown.
   *
   * <p>The retry only handles transaction contention error. It does not handle other error cases,
   * e.g. disconnection or timeout.
   *
   * <p>Generally this is used to wrap multiple cockroachDB statements producing some expected
   * return value.
   *
   * @param function The function to apply with a new transactional {@link Connection}
   * @param <R> The return type.
   * @return The result of {@code TransactionalFunction#apply(Connection)}
   * @throws ApplicationException If any errors occur.
   */
  protected <R> R withRetryableTransaction(final TransactionalFunction<R> function) {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false); // manually manage the commit lifecycle
      int retryCount = 0;
      while (true) {
        Savepoint sp = connection.setSavepoint(SAVEPOINT_NAME);
        try {
          R result = function.apply(connection);
          connection.releaseSavepoint(sp);
          connection.commit();
          return result;
        } catch (SQLException e) {
          if (retryCount < maxRetries && RETRY_SQL_STATE.equals(e.getSQLState())) {
            LOG.warn(
                "retryable exception occurred: sql state = [{}], message = [{}], retry counter = {}",
                e.getSQLState(),
                e.getMessage(),
                retryCount);
            connection.rollback(sp);
            retryCount++;
            int sleepMillis =
                Math.min(
                    maxRetryDelay,
                    (int) (Math.pow(2, retryCount) * initialRetryDelay)
                        + ThreadLocalRandom.current().nextInt(initialRetryDelay));
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
          } else {
            LOG.warn(
                "non-retryable exception occurred: sql state = [{}], message = [{}], retry counter = {}",
                e.getSQLState(),
                e.getMessage(),
                retryCount);
            connection.rollback();
            throw e;
          }
        }
      }
    } catch (SQLException e) {
      LOG.warn(
          "non-retryable exception occurred: sql state = [{}], message = [{}]",
          e.getSQLState(),
          e.getMessage());
      if (e.getMessage() != null
          && e.getMessage().startsWith("ERROR: split failed while applying backpressure to")) {
        Monitors.error(this.getClass().getName(), "nonretryable_mvcc_error");
      } else {
        Monitors.error(this.getClass().getName(), "retryable_transaction_error_" + e.getSQLState());
      }
      throw new ApplicationException(BACKEND_ERROR, e.getMessage(), e);
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException exception occurred: message = [{}]", e.getMessage());
      Thread.currentThread().interrupt();
      throw new ApplicationException(INTERNAL_ERROR, e.getMessage(), e);
    }
  }

  /**
   * Initialize a new transactional {@link Connection} from {@link #dataSource}, prepare a new SQL
   * statement, and pass then it to {@literal function}. It includes a retry mechanism as
   * CockroachDB recommend to implement client side retry
   *
   * <p>Successful executions of {@literal function} will result in a commit and return of {@link
   * StatementFunction#apply)}.
   *
   * <p>If SQL exception has a retry sql state without exhausting the retry chances, the transaction
   * rollback to the save point and retry with a new prepared statement. Otherwise, the error will
   * be thrown.
   *
   * <p>The retry only handles transaction contention error. It does not handle other error cases,
   * e.g. disconnection or timeout.
   *
   * <p>Generally this is used to wrap a {@link PreparedStatement} producing some expected return
   * value.
   *
   * @param function The function to apply with a new {@link PreparedStatement} in a transactional
   *     {@link Connection}
   * @param <R> The return type.
   * @return The result of {@code StatementFunction#apply()}
   * @throws ApplicationException If any errors occur.
   */
  protected <R> R withRetryableStatement(
      final String statement, final StatementFunction<R> function) {
    return withRetryableTransaction(
        conn -> {
          try (PreparedStatement stmt = conn.prepareStatement(statement)) {
            return function.apply(stmt);
          }
        });
  }

  /**
   * Get time from the timestamp field if present.
   *
   * @param rs The result set.
   * @param field Timestamp field.
   * @return The timestamp value in long if not null.
   * @throws SQLException sql exception
   */
  protected Long getTimestampIfPresent(ResultSet rs, String field) throws SQLException {
    Timestamp timestamp = rs.getTimestamp(field);
    if (timestamp != null) {
      return timestamp.getTime();
    }
    return null;
  }
}
