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

import lombok.Getter;
import lombok.Setter;

/**
 * SEL evaluator properties. Please check {@link com.netflix.sel.SelEvaluator} about how they are
 * used.
 */
@Getter
@Setter
public class SelProperties {
  private int threadNum;
  private int timeoutMillis;
  private int stackLimit;
  private int loopLimit;
  private int arrayLimit;
  private int lengthLimit;
  private long visitLimit;
  private long memoryLimit;
}
