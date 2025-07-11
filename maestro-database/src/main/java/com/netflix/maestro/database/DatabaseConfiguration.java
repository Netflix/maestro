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

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("JavadocVariable")
public interface DatabaseConfiguration {

  String JDBC_URL_PROPERTY_NAME = "jdbc.url";
  String JDBC_URL_DEFAULT_VALUE = "jdbc:postgresql://localhost:5432/maestro";

  String DB_TYPE_PROPERTY_NAME = "db.type";
  String DB_TYPE_DEFAULT_VALUE = "postgres";

  String JDBC_USER_NAME_PROPERTY_NAME = "jdbc.username";
  String JDBC_USER_NAME_DEFAULT_VALUE = "maestro";

  String JDBC_PASSWORD_PROPERTY_NAME = "jdbc.password";
  String JDBC_PASSWORD_DEFAULT_VALUE = "password";

  String FLYWAY_ENABLED_PROPERTY_NAME = "flyway.enabled";
  boolean FLYWAY_ENABLED_DEFAULT_VALUE = true;

  String FLYWAY_BASELINE_MIGRATION_ENABLED_PROPERTY_NAME = "flyway.baseline.migration.enabled";
  boolean FLYWAY_BASELINE_MIGRATION_ENABLED_DEFAULT_VALUE = false;

  String FLYWAY_TABLE_PROPERTY_NAME = "flyway.table";

  // The defaults are currently in line with the HikariConfig defaults, which are unfortunately
  // private.
  String CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME = "workflow.database.connection.pool.size.max";
  int CONNECTION_POOL_MAX_SIZE_DEFAULT_VALUE = -1;

  String CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME = "workflow.database.connection.pool.idle.min";
  int CONNECTION_POOL_MINIMUM_IDLE_DEFAULT_VALUE = -1;

  String CONNECTION_MAX_LIFETIME_PROPERTY_NAME = "workflow.database.connection.lifetime.max";
  long CONNECTION_MAX_LIFETIME_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(30);

  String CONNECTION_IDLE_TIMEOUT_PROPERTY_NAME = "workflow.database.connection.idle.timeout";
  long CONNECTION_IDLE_TIMEOUT_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(10);

  String CONNECTION_TIMEOUT_PROPERTY_NAME = "workflow.database.connection.timeout";
  long CONNECTION_TIMEOUT_DEFAULT_VALUE = TimeUnit.SECONDS.toMillis(30);

  String AUTO_COMMIT_PROPERTY_NAME = "workflow.database.autocommit";
  // This is consistent with the current default when building the Hikari Client.
  boolean AUTO_COMMIT_DEFAULT_VALUE = false;

  String DB_INSERT_BATCH_SIZE_PROPERTY_NAME = "workflow.database.insert.batch.size";
  int DB_INSERT_BATCH_SIZE_DEFAULT_VALUE = 64;

  String DB_MAX_SEARCH_SIZE_PROPERTY_NAME = "workflow.database.max.search.size";
  int DB_MAX_SEARCH_SIZE_DEFAULT_VALUE = 1000;

  String DB_ERROR_RETRIES_PROPERTY_NAME = "workflow.database.error.retries";
  int DB_ERROR_RETRIES_DEFAULT_VALUE = 3;

  String DB_MAX_RETRY_DELAY_PROPERTY_NAME = "workflow.database.max.retry.delay";
  int DB_MAX_RETRY_DELAY_DEFAULT_VALUE = 1000;

  String DB_INITIAL_RETRY_DELAY_PROPERTY_NAME = "workflow.database.initial.retry.delay";
  int DB_INITIAL_RETRY_DELAY_DEFAULT_VALUE = 100;

  String DB_CLIENT_NAME_PROPERTY_NAME = "workflow.database.client.name";
  String DB_CLIENT_NAME_DEFAULT_VALUE = JDBC_USER_NAME_DEFAULT_VALUE;

  String REWRITE_BATCHED_INSERTS_PROPERTY_NAME = "workflow.database.rewrite.batched.inserts";
  boolean REWRITE_BATCHED_INSERTS_DEFAULT_VALUE = true;

  String DB_LOGIN_TIMEOUT_PROPERTY_NAME = "workflow.database.login.timeout.seconds";
  int DB_LOGIN_TIMEOUT_DEFAULT_VALUE = 5;

  String DB_SOCKET_TIMEOUT_PROPERTY_NAME = "workflow.database.socket.timeout.seconds";
  int DB_SOCKET_TIMEOUT_DEFAULT_VALUE = 30;

  default String getJdbcUrl() {
    return getProperty(JDBC_URL_PROPERTY_NAME, JDBC_URL_DEFAULT_VALUE);
  }

  default String getDbType() {
    return getProperty(DB_TYPE_PROPERTY_NAME, DB_TYPE_DEFAULT_VALUE);
  }

  default String getJdbcUserName() {
    return getProperty(JDBC_USER_NAME_PROPERTY_NAME, JDBC_USER_NAME_DEFAULT_VALUE);
  }

  default String getJdbcPassword() {
    return getProperty(JDBC_PASSWORD_PROPERTY_NAME, JDBC_PASSWORD_DEFAULT_VALUE);
  }

  default boolean isFlywayEnabled() {
    return getBooleanProperty(FLYWAY_ENABLED_PROPERTY_NAME, FLYWAY_ENABLED_DEFAULT_VALUE);
  }

  default boolean isFlywayBaseLineMigrationEnabled() {
    return getBooleanProperty(
        FLYWAY_BASELINE_MIGRATION_ENABLED_PROPERTY_NAME,
        FLYWAY_BASELINE_MIGRATION_ENABLED_DEFAULT_VALUE);
  }

  default Optional<String> getFlywayTable() {
    return Optional.ofNullable(getProperty(FLYWAY_TABLE_PROPERTY_NAME, null));
  }

  default int getConnectionPoolMaxSize() {
    return getIntProperty(
        CONNECTION_POOL_MAX_SIZE_PROPERTY_NAME, CONNECTION_POOL_MAX_SIZE_DEFAULT_VALUE);
  }

  default int getConnectionPoolMinIdle() {
    return getIntProperty(
        CONNECTION_POOL_MINIMUM_IDLE_PROPERTY_NAME, CONNECTION_POOL_MINIMUM_IDLE_DEFAULT_VALUE);
  }

  default long getConnectionMaxLifetime() {
    return getLongProperty(
        CONNECTION_MAX_LIFETIME_PROPERTY_NAME, CONNECTION_MAX_LIFETIME_DEFAULT_VALUE);
  }

  default long getConnectionIdleTimeout() {
    return getLongProperty(
        CONNECTION_IDLE_TIMEOUT_PROPERTY_NAME, CONNECTION_IDLE_TIMEOUT_DEFAULT_VALUE);
  }

  default long getConnectionTimeout() {
    return getLongProperty(CONNECTION_TIMEOUT_PROPERTY_NAME, CONNECTION_TIMEOUT_DEFAULT_VALUE);
  }

  default boolean isAutoCommit() {
    return getBooleanProperty(AUTO_COMMIT_PROPERTY_NAME, AUTO_COMMIT_DEFAULT_VALUE);
  }

  default int getDbInsertBatchSize() {
    return getIntProperty(DB_INSERT_BATCH_SIZE_PROPERTY_NAME, DB_INSERT_BATCH_SIZE_DEFAULT_VALUE);
  }

  default int getDbMaxSearchSize() {
    return getIntProperty(DB_MAX_SEARCH_SIZE_PROPERTY_NAME, DB_MAX_SEARCH_SIZE_DEFAULT_VALUE);
  }

  default int getDbErrorRetries() {
    return getIntProperty(DB_ERROR_RETRIES_PROPERTY_NAME, DB_ERROR_RETRIES_DEFAULT_VALUE);
  }

  default int getDbMaxRetryDelay() {
    return getIntProperty(DB_MAX_RETRY_DELAY_PROPERTY_NAME, DB_MAX_RETRY_DELAY_DEFAULT_VALUE);
  }

  default int getDbInitialRetryDelay() {
    return getIntProperty(
        DB_INITIAL_RETRY_DELAY_PROPERTY_NAME, DB_INITIAL_RETRY_DELAY_DEFAULT_VALUE);
  }

  default String getClientName() {
    return getProperty(DB_CLIENT_NAME_PROPERTY_NAME, DB_CLIENT_NAME_DEFAULT_VALUE);
  }

  default boolean isRewriteBatchedInserts() {
    return getBooleanProperty(
        REWRITE_BATCHED_INSERTS_PROPERTY_NAME, REWRITE_BATCHED_INSERTS_DEFAULT_VALUE);
  }

  default int getDbLoginTimeout() {
    return getIntProperty(DB_LOGIN_TIMEOUT_PROPERTY_NAME, DB_LOGIN_TIMEOUT_DEFAULT_VALUE);
  }

  default int getDbSocketTimeout() {
    return getIntProperty(DB_SOCKET_TIMEOUT_PROPERTY_NAME, DB_SOCKET_TIMEOUT_DEFAULT_VALUE);
  }

  default String getProperty(String key, String defaultValue) {
    var val = System.getenv(key.replace('.', '_'));
    if (val == null || val.isEmpty()) {
      val = Optional.ofNullable(System.getProperty(key)).orElse(defaultValue);
    }
    return val;
  }

  default int getIntProperty(String key, int defaultValue) {
    var val = this.getProperty(key, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  default long getLongProperty(String key, long defaultValue) {
    var val = this.getProperty(key, String.valueOf(defaultValue));
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  default boolean getBooleanProperty(String key, boolean defaultValue) {
    var val = this.getProperty(key, null);
    return val != null ? Boolean.parseBoolean(val) : defaultValue;
  }
}
