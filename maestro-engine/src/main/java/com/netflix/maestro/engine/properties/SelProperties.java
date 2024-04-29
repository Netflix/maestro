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
 * SEL evaluator properties. Please check {@link com.netflix.sel.SelEvaluator} about how they are
 * used.
 */
@Getter
@AllArgsConstructor
@ToString
@Builder
public class SelProperties {
  private final int threadNum;
  private final int timeoutMillis;
  private final int stackLimit;
  private final int loopLimit;
  private final int arrayLimit;
  private final int lengthLimit;
  private final long visitLimit;
  private final long memoryLimit;
}
