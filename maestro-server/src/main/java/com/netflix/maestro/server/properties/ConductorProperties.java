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
package com.netflix.maestro.server.properties;

import com.netflix.maestro.engine.properties.MaestroConductorProperties;
import java.util.Locale;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Conductor configuration properties. */
@AllArgsConstructor
@ConfigurationProperties(prefix = "conductor")
public class ConductorProperties extends MaestroConductorProperties {

  private final Map<String, String> configs;

  /**
   * Get conductor properties from Spring boot configs.
   *
   * @param name Name of the property
   * @param defaultValue Default value when not specified
   * @return User defined string property.
   */
  @Override
  public String getProperty(String name, String defaultValue) {
    String key = name.replace('.', '-').toLowerCase(Locale.US);
    return configs.getOrDefault(key, defaultValue);
  }
}
