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
package com.netflix.maestro.extensions;

import static org.mockito.Mockito.mock;

import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.database.DatabaseConfiguration;
import com.netflix.maestro.database.MaestroDatabaseHelper;
import com.netflix.maestro.metrics.MaestroMetrics;
import javax.sql.DataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Base class for DAO tests in the extensions module. Uses Postgres via testcontainers. */
@SuppressFBWarnings("MS_CANNOT_BE_FINAL")
public class ExtensionsDaoBaseTest extends ExtensionsBaseTest {
  protected static DatabaseConfiguration config;
  protected static DataSource dataSource;
  protected static MaestroMetrics metrics;

  @BeforeClass
  public static void init() {
    ExtensionsBaseTest.init();
    config = MaestroDatabaseHelper.getConfig();
    dataSource = MaestroDatabaseHelper.getDataSource();
    metrics = mock(MaestroMetrics.class);
  }

  @AfterClass
  public static void destroy() {
    ExtensionsBaseTest.destroy();
  }
}
