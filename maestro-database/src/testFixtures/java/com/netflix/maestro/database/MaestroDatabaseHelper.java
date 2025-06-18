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
package com.netflix.maestro.database;

import javax.sql.DataSource;

/** Maestro database helper utils. */
public interface MaestroDatabaseHelper {
  /** The size of the connection pool for tests. */
  int TEST_CONNECTION_POLL_SIZE = 10;

  class MaestroDBTestConfiguration implements DatabaseConfiguration {
    @Override
    public String getJdbcUrl() {
      return "jdbc:tc:postgresql:17:///maestro";
    }

    @Override
    public String getDbType() {
      return "postgres";
    }

    @Override
    public int getConnectionPoolMaxSize() {
      return TEST_CONNECTION_POLL_SIZE;
    }

    @Override
    public int getConnectionPoolMinIdle() {
      return getConnectionPoolMaxSize();
    }
  }

  /** get database config. */
  static DatabaseConfiguration getConfig() {
    return new MaestroDBTestConfiguration();
  }

  /** get database source. */
  static DataSource getDataSource() {
    return new DatabaseSourceProvider(getConfig()).get();
  }
}
