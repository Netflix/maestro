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
package com.netflix.maestro.engine.metrics;

/** Maestro metrics interface to record metrics. */
public interface MaestroMetrics {

  /**
   * Increment a counter with a given name for class and tags.
   *
   * @param name metric name
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  void counter(String name, Class<?> clazz, String... tags);

  /**
   * Set a gauge with a given name for class and tags.
   *
   * @param name metric name
   * @param value value to set
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  void gauge(String name, double value, Class<?> clazz, String... tags);

  /**
   * Record a value for a distribution summary.
   *
   * @param name metric name
   * @param value value to record
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  void distributionSummary(String name, long value, Class<?> clazz, String... tags);

  /**
   * Update the statistics with the specified amount in milliseconds.
   *
   * @param name metric name
   * @param duration duration to update
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  void timer(String name, long duration, Class<?> clazz, String... tags);
}
