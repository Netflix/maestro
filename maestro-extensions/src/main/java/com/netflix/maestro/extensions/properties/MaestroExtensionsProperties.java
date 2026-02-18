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
package com.netflix.maestro.extensions.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration properties for the Maestro Extensions service. */
@Getter
@Setter
@ConfigurationProperties(prefix = "extensions")
public class MaestroExtensionsProperties {
  /** Base URL for calling maestro-server REST API (e.g. http://localhost:8080/api/v3). */
  private String maestroBaseUrl;

  /** SQS queue URL for consuming maestro events. */
  private String maestroEventQueueUrl;
}
