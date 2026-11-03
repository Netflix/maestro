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

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties for overriding or extending the bundled default parameters via application config.
 *
 * <p>Each field carries a YAML document (as text) with the same shape as the bundled {@code
 * defaultparams/*.yaml} resources. Values are parsed by the maestro YAML {@code ObjectMapper} and
 * merged, by param name, on top of the bundled defaults. Absent fields leave the bundled defaults
 * untouched.
 */
@AllArgsConstructor
@Getter
@ConfigurationProperties(prefix = "maestro.default-params")
public class DefaultParamsProperties {
  /** Override blob for the default workflow (system) params. */
  private final String workflow;

  /** Override blob for the default step params. */
  private final String step;

  /** Override blob for the default dry-run params. */
  private final String dryRun;

  /** Override blobs keyed by step-type name (e.g. {@code subworkflow}, {@code foreach}). */
  private final Map<String, String> byType;
}
