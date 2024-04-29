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
package com.netflix.maestro.engine.properties;

import com.netflix.conductor.cockroachdb.CockroachDBConfiguration;
import com.netflix.conductor.core.config.SystemPropertiesConfiguration;
import java.util.concurrent.TimeUnit;

/** Maestro Conductor configuration properties. */
public class MaestroConductorProperties extends SystemPropertiesConfiguration
    implements CockroachDBConfiguration {

  private static final String COMPRESSION_ENABLED_PROPERTY_NAME = "payload.compression.enabled";
  private static final boolean COMPRESSION_DEFAULT_VALUE = false;
  private static final String COMPRESSOR_NAME_PROPERTY_NAME = "payload.compressor.name";
  private static final String COMPRESSOR_NAME_DEFAULT_VALUE = "gzip";

  private static final String MAX_TASK_UPDATE_INTERVAL_PROPERTY_NAME =
      "max.task.update.interval.ms";
  private static final long MAX_TASK_UPDATE_INTERVAL_DEFAULT_VALUE = TimeUnit.MINUTES.toMillis(2);

  public boolean isCompressionEnabled() {
    return getBooleanProperty(COMPRESSION_ENABLED_PROPERTY_NAME, COMPRESSION_DEFAULT_VALUE);
  }

  public String getCompressorName() {
    return getProperty(COMPRESSOR_NAME_PROPERTY_NAME, COMPRESSOR_NAME_DEFAULT_VALUE);
  }

  public long getMaxTaskUpdateIntervalInMillis() {
    return getLongProperty(
        MAX_TASK_UPDATE_INTERVAL_PROPERTY_NAME, MAX_TASK_UPDATE_INTERVAL_DEFAULT_VALUE);
  }
}
