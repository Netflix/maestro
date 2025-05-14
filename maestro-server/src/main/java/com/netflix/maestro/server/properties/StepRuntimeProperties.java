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

import com.netflix.maestro.engine.properties.CallbackDelayConfig;
import com.netflix.maestro.engine.properties.ForeachStepRuntimeProperties;
import com.netflix.maestro.engine.properties.SubworkflowStepRuntimeProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Properties for step runtime. */
@AllArgsConstructor
@Getter
@ConfigurationProperties(prefix = "stepruntime")
public class StepRuntimeProperties {
  private final String env;

  private final ForeachStepRuntimeProperties foreach;
  private final SubworkflowStepRuntimeProperties subworkflow;

  private final Cache cache;

  private final CallbackDelayConfig callbackDelayConfig;

  /** Cache Properties. */
  @Getter
  @Setter
  public static class Cache {
    private long defaultShaCacheMinutes;
    private long customShaCacheMinutes;
    private int defaultShaMaxSize;
    private int customShaMaxSize;
  }
}
