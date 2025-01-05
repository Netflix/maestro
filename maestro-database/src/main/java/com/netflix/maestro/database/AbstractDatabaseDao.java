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
package com.netflix.maestro.database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.database.utils.ConnectionFunction;
import com.netflix.maestro.database.utils.ResultProcessor;
import com.netflix.maestro.database.utils.StatementFunction;
import com.netflix.maestro.database.utils.StatementPreparer;
import com.netflix.maestro.exceptions.MaestroDatabaseError;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.metrics.MaestroMetrics;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

/**
 * Database Base DAO to support basic DB operation.
 *
 * @author jun-he
 */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@Slf4j
public abstract class AbstractDatabaseDao {
  private static final String RETRY_SQL_STATE = "40001";
  private static final String QUERY_ERROR_METRIC_NAME = "maestro_db_query_error";
  private static final String ERROR_TYPE_TAG_NAME = "errorType";

  protected static final String PAYLOAD_COLUMN = "payload";
  protected static final String ID_COLUMN = "id";
  protected static final String STATUS_COLUMN = "status";
  protected static final int SUCCESS_WRITE_SIZE = 1;
  protected static final String ARRAY_TYPE_NAME = "TEXT";

  private final ObjectMapper objectMapper;
  private final DataSource dataSource;
  private final MaestroMetrics metrics;
  private final int maxRetries;
  private final int maxRetryDelay;
  private final int initialRetryDelay;

  public AbstractDatabaseDao(
      DataSource dataSource,
      ObjectMapper objectMapper,
      DatabaseConfiguration config,
      MaestroMetrics metrics) {
    this.dataSource = dataSource;
    this.objectMapper = objectMapper;
    this.metrics = metrics;
    this.maxRetries = config.getDbErrorRetries();
    this.maxRetryDelay = config.getDbMaxRetryDelay();
    this.initialRetryDelay = config.getDbInitialRetryDelay();
  }

  protected <T> List<T> getPayloads(String stmt, StatementPreparer preparer, Class<T> clazz) {
    return withRetryableQuery(stmt, preparer, r -> payloadsFromResult(r, clazz));
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

  /**
   * Convert the object to the JSON string.
   *
   * @param value the object value
   * @return serialized string
   */
  protected String toJson(Object value) {
    try {
      return objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new MaestroUnprocessableEntityException(
          "cannot write an object to json string due to [%s]", e.getMessage());
    }
  }

  /**
   * Parse a JSON to a given class instance.
   *
   * @param json json string
   * @return deserialized object
   */
  protected <T> T fromJson(String json, Class<T> clazz) {
    try {
      return objectMapper.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new MaestroUnprocessableEntityException(
          "cannot read an object from json string due to [%s]", e.getMessage());
    }
  }

  /**
   * Parse a JSON to a given TypeReference instance.
   *
   * @param json json string
   * @return deserialized object
   */
  protected <T> T fromJson(String json, TypeReference<T> valueTypeRef) {
    try {
      return objectMapper.readValue(json, valueTypeRef);
    } catch (JsonProcessingException e) {
      throw new MaestroUnprocessableEntityException(
          "cannot read a valueRef from json string due to [%s]", e.getMessage());
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
      metrics.counter(QUERY_ERROR_METRIC_NAME, this.getClass(), "methodName", methodName);
      LOG.error("{} due to {}", log, combinedArgs);
      throw e;
    }
  }

  /**
   * Initialize a new transactional {@link Connection} PreparedStatement {@link PreparedStatement}
   * from {@link #dataSource} and pass it to {@literal statement preparer}. After querying the
   * results from DB, it then passes them to a processor. It includes a retry mechanism for client
   * side retry.
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
   * {@literal function}. It includes a retry mechanism for client side retry
   *
   * <p>Successful executions of {@literal function} will result in a commit and return of {@link
   * ConnectionFunction#apply)}.
   *
   * <p>If SQL exception has a retry sql state without exhausting the retry chances, the transaction
   * will be rollback and retried. Otherwise, the error will be thrown.
   *
   * <p>The retry only handles transaction contention error. It does not handle other error cases,
   * e.g. disconnection or timeout.
   *
   * <p>Generally this is used to wrap multiple database statements producing some expected return
   * value.
   *
   * @param function The function to apply with a new transactional {@link Connection}
   * @param <R> The return type.
   * @return The result of {@code TransactionalFunction#apply(Connection)}
   * @throws MaestroInternalError If any errors occur.
   */
  protected <R> R withRetryableTransaction(final ConnectionFunction<R> function) {
    try {
      int retryCount = 0;
      while (true) {
        try {
          return withTransaction(function);
        } catch (SQLException e) {
          if (retryCount < maxRetries && RETRY_SQL_STATE.equals(e.getSQLState())) {
            LOG.warn(
                "retryable exception occurred: sql state = [{}], message = [{}], retry counter = {}",
                e.getSQLState(),
                e.getMessage(),
                retryCount);
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
        metrics.counter(
            QUERY_ERROR_METRIC_NAME,
            this.getClass(),
            ERROR_TYPE_TAG_NAME,
            "nonretryable_mvcc_error");
      } else {
        metrics.counter(
            QUERY_ERROR_METRIC_NAME,
            this.getClass(),
            ERROR_TYPE_TAG_NAME,
            "retryable_transaction_error_" + e.getSQLState());
      }
      throw new MaestroDatabaseError(e, e.getMessage());
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException exception occurred: message = [{}]", e.getMessage());
      Thread.currentThread().interrupt();
      throw new MaestroInternalError(e, e.getMessage());
    }
  }

  /**
   * Initialize a new transactional {@link Connection} from {@link #dataSource} and pass it to
   * {@literal function}.
   *
   * <p>Successful executions of {@literal function} will result in a commit and return of {@link
   * ConnectionFunction#apply)}.
   *
   * <p>Generally this is used to wrap multiple database statements producing some expected return
   * value.
   *
   * @param function The function to apply with a new transactional {@link Connection}
   * @param <R> The return type.
   * @return The result of {@code TransactionalFunction#apply(Connection)}
   * @throws MaestroInternalError If any errors occur.
   */
  private <R> R withTransaction(final ConnectionFunction<R> function) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false); // manually manage the commit lifecycle
      try {
        R result = function.apply(connection);
        connection.commit();
        return result;
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    }
  }

  /**
   * Initialize a new transactional {@link Connection} from {@link #dataSource}, prepare a new SQL
   * statement, and pass then it to {@literal function}. It includes a retry mechanism for client
   * side retry.
   *
   * <p>Successful executions of {@literal function} will result in a commit and return of {@link
   * StatementFunction#apply)}.
   *
   * <p>If SQL exception has a retry sql state without exhausting the retry chances, the transaction
   * is rollback and retried with a new prepared statement. Otherwise, the error will be thrown.
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
   * @throws MaestroInternalError If any errors occur.
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
}
