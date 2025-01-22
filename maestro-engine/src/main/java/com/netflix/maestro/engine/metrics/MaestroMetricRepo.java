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

import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.utils.Checks;
import com.netflix.spectator.api.Clock;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.DistributionSummary;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.histogram.PercentileTimer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

/** Metric repo class to record metrics. */
@AllArgsConstructor
public class MaestroMetricRepo implements MaestroMetrics {
  private final Registry registry;

  private final Map<String, Map<List<Tag>, Counter>> counters = new ConcurrentHashMap<>();

  private final Map<String, Map<List<Tag>, PercentileTimer>> timers = new ConcurrentHashMap<>();

  private final Map<String, Map<List<Tag>, Gauge>> gauges = new ConcurrentHashMap<>();

  private final Map<String, Map<List<Tag>, DistributionSummary>> distributionSummaries =
      new ConcurrentHashMap<>();

  /** For testing purpose. */
  public void reset() {
    counters.clear();
    timers.clear();
    gauges.clear();
    distributionSummaries.clear();
    ((DefaultRegistry) registry).reset();
  }

  /**
   * Increment a counter with a given name for class and tags.
   *
   * @param name metric name
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  @Override
  public void counter(String name, Class<?> clazz, String... tags) {
    getCounter(name, clazz, tags).increment();
  }

  /** For testing purpose. */
  public Counter getCounter(String name, Class<?> clazz, String... tags) {
    final List<Tag> metricTags = toTags(clazz, tags);
    return counters
        .computeIfAbsent(name, v -> new ConcurrentHashMap<>())
        .computeIfAbsent(
            metricTags,
            m -> {
              Id id = registry.createId(name, m);
              return registry.counter(id);
            });
  }

  /**
   * Set a gauge with a given name for class and tags.
   *
   * @param name metric name
   * @param value value to set
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  @Override
  public void gauge(String name, double value, Class<?> clazz, String... tags) {
    final List<Tag> metricTags = toTags(clazz, tags);
    Gauge gauge =
        gauges
            .computeIfAbsent(name, v -> new ConcurrentHashMap<>())
            .computeIfAbsent(
                metricTags,
                m -> {
                  Id id = registry.createId(name, m);
                  return registry.gauge(id);
                });
    gauge.set(value);
  }

  /**
   * Record a value for a distribution summary.
   *
   * @param name metric name
   * @param value value to record
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  @Override
  public void distributionSummary(String name, long value, Class<?> clazz, String... tags) {
    final List<Tag> metricTags = toTags(clazz, tags);
    DistributionSummary summary =
        distributionSummaries
            .computeIfAbsent(name, v -> new ConcurrentHashMap<>())
            .computeIfAbsent(
                metricTags,
                m -> {
                  Id id = registry.createId(name, m);
                  return registry.distributionSummary(id);
                });
    summary.record(value);
  }

  /**
   * Update the statistics with the specified amount in milliseconds.
   *
   * @param name metric name
   * @param duration duration to update
   * @param clazz class info to add it to a tag
   * @param tags other tags to add
   */
  @Override
  public void timer(String name, long duration, Class<?> clazz, String... tags) {
    final List<Tag> metricTags = toTags(clazz, tags);
    Timer timer =
        timers
            .computeIfAbsent(name, v -> new ConcurrentHashMap<>())
            .computeIfAbsent(
                metricTags,
                m -> {
                  Id id = registry.createId(name, m);
                  return PercentileTimer.get(registry, id);
                });
    timer.record(duration, TimeUnit.MILLISECONDS);
  }

  public Clock clock() {
    return registry.clock();
  }

  private List<Tag> toTags(Class<?> clazz, String... tags) {
    Checks.checkTrue(
        tags.length % 2 == 0,
        () ->
            String.format(
                "Invalid tags %s and they must be <key, value> pairs", Arrays.toString(tags)));
    Stream.Builder<Tag> builder = Stream.builder();
    builder.add(Tag.of("class", clazz.getSimpleName()));
    for (int i = 0; i < tags.length; i += 2) {
      String tag = Checks.notNull(tags[i], "Tag name cannot be null or empty for class %s", clazz);
      String value = tags[i + 1];
      builder.add(Tag.of(tag, value == null ? "unknown" : value));
    }
    return builder
        .build()
        .sorted(Comparator.comparing(Tag::key).thenComparing(Tag::value))
        .collect(Collectors.toList());
  }
}
