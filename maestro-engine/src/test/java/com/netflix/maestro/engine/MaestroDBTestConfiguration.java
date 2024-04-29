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
package com.netflix.maestro.engine;

import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;

/** Maestro DB configuration for testing. */
public class MaestroDBTestConfiguration extends SystemPropertiesConfiguration
    implements CockroachDBConfiguration {
  @Override
  public boolean isFollowerReadsEnabled() {
    return false;
  }

  @Override
  public String getJdbcUrl() {
    return "jdbc:tc:cockroach:v22.1.16:///maestro";
  }

  @Override
  public int getConnectionPoolMaxSize() {
    return 10;
  }

  @Override
  public int getConnectionPoolMinIdle() {
    return getConnectionPoolMaxSize();
  }
}
