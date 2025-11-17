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
package com.netflix.maestro.engine.properties;

import java.util.Set;
import lombok.Getter;
import lombok.Setter;

/**
 * Http stepruntime properties. Please check {@link
 * com.netflix.maestro.engine.stepruntime.HttpStepRuntime} about how they are used.
 */
@Getter
@Setter
public class HttpStepProperties {
  private long connectionTimeout; // in milliseconds
  private long sendTimeout; // in milliseconds
  private long maxResponseSize; // in bytes

  /** List of allowed hostnames for SSRF protection. HTTP step only allows these hostnames. */
  private Set<String> allowList;
}
