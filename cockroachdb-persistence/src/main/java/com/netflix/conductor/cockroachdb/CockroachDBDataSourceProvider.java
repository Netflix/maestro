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
package com.netflix.conductor.cockroachdb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.ThreadFactory;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** cockroach db data source provider */
public class CockroachDBDataSourceProvider {
  private static final Logger logger = LoggerFactory.getLogger(CockroachDBDataSourceProvider.class);

  private final CockroachDBConfiguration configuration;

  public CockroachDBDataSourceProvider(CockroachDBConfiguration configuration) {
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
      logger.error("error migration DB", t);
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
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("hikari-cockroachdb-%d").build();

    cfg.setThreadFactory(tf);
    return cfg;
  }

  // todo add complete lifecycle for the connection, i.e. startup and shutdown.
  private void flywayMigrate(DataSource dataSource) {
    boolean enabled = configuration.isFlywayEnabled();
    if (!enabled) {
      logger.debug("Flyway migrations are disabled");
      return;
    }

    Flyway flyway =
        Flyway.configure()
            .dataSource(dataSource)
            .placeholderReplacement(false)
            .baselineOnMigrate(configuration.isFlywayBaseLineMigrationEnabled())
            .load();

    flyway.migrate();
  }
}
