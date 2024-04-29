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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Foreach Properties. Please check the method of launchForeachIterations in {@link
 * com.netflix.maestro.engine.steps.ForeachStepRuntime} about how they are used.
 */
@Getter
@AllArgsConstructor
@ToString
@Builder
public class ForeachStepRuntimeProperties {
  private final int loopBatchLimit;
  private final int insertBatchLimit;
  private final int runJobBatchLimit;
  private final int getRollupBatchLimit;
}
