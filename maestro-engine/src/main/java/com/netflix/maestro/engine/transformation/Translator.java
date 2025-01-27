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
package com.netflix.maestro.engine.transformation;

/**
 * Translator interface.
 *
 * @param <T> Source data model type.
 * @param <R> Destination data model type.
 */
public interface Translator<T, R> {
  /** flow engine task default polling interval in seconds. */
  int DEFAULT_FLOW_TASK_DELAY = 0;

  /** flow engine task default reconciliation interval in seconds. */
  int DEFAULT_FLOW_TASK_RECONCILIATION_INTERVAL = 60;

  /** flow reference formatter to convert workflow identifier to a flow reference. */
  String FLOW_REFERENCE_FORMATTER = "[%s][%s][%s]";

  /**
   * Translate function.
   *
   * @param definition source definition data
   * @return translated data
   */
  R translate(T definition);
}
