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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.flywaydb.core.Flyway;

/** database data source provider. */
@Slf4j
public class DatabaseSourceProvider {
  private final DatabaseConfiguration configuration;

  public DatabaseSourceProvider(DatabaseConfiguration configuration) {
    this.configuration = configuration;
  }

  public DataSource get() {
    HikariDataSource dataSource = null;
    try {
      dataSource = new HikariDataSource(createConfiguration());
      flywayMigrate(dataSource);
      return dataSource;
    } catch (final Throwable t) {
      if (null != dataSource && !dataSource.isClosed()) {
        dataSource.close();
      }
      LOG.error("error migration DB", t);
      throw t;
    }
  }

  private HikariConfig createConfiguration() {
    HikariConfig cfg = new HikariConfig();
    cfg.setJdbcUrl(configuration.getJdbcUrl());
    cfg.setUsername(configuration.getJdbcUserName());
    cfg.setPassword(configuration.getJdbcPassword());
    cfg.setAutoCommit(false);
    cfg.setMaximumPoolSize(configuration.getConnectionPoolMaxSize());
    cfg.setMinimumIdle(configuration.getConnectionPoolMinIdle());
    cfg.setMaxLifetime(configuration.getConnectionMaxLifetime());
    cfg.setIdleTimeout(configuration.getConnectionIdleTimeout());
    cfg.setConnectionTimeout(configuration.getConnectionTimeout());
    cfg.setAutoCommit(configuration.isAutoCommit());
    cfg.addDataSourceProperty("ApplicationName", configuration.getClientName());
    cfg.addDataSourceProperty("rewriteBatchedInserts", configuration.isRewriteBatchedInserts());
    cfg.addDataSourceProperty("loginTimeout", configuration.getDbLoginTimeout());
    cfg.addDataSourceProperty("socketTimeout", configuration.getDbSocketTimeout());

    ThreadFactory tf =
        new ThreadFactory() {
          private final AtomicInteger threadNumber = new AtomicInteger(1);

          @Override
          public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("hikari-db-" + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
          }
        };

    cfg.setThreadFactory(tf);
    return cfg;
  }

  // todo add complete lifecycle for the connection, i.e. startup and shutdown.
  private void flywayMigrate(DataSource dataSource) {
    boolean enabled = configuration.isFlywayEnabled();
    if (!enabled) {
      LOG.debug("Flyway migrations are disabled");
      return;
    }

    Flyway flyway =
        Flyway.configure()
            .locations("classpath:db/migration/" + configuration.getDbType())
            .dataSource(dataSource)
            .placeholderReplacement(false)
            .baselineOnMigrate(configuration.isFlywayBaseLineMigrationEnabled())
            .load();

    flyway.migrate();
  }
}
