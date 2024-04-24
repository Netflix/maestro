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
package com.netflix.maestro.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/** Map helper utility class. */
public final class MapHelper {
  private MapHelper() {}

  /** Collector to create a {@link LinkedHashMap} object. */
  public static <T, K, U> Collector<T, ?, Map<K, U>> toListMap(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
    return Collectors.toMap(
        keyMapper,
        valueMapper,
        (u, v) -> {
          throw new IllegalArgumentException(
              String.format("There is a duplicate key with values [%s] and [%s]", u, v));
        },
        LinkedHashMap::new);
  }

  /**
   * Collector to create a {@link LinkedHashMap} object that ignores duplicate keys by picking
   * arbitrary key.
   */
  public static <T, K, U> Collector<T, ?, Map<K, U>> toListMapIgnoringDuplicateKeys(
      Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
    return Collectors.toMap(keyMapper, valueMapper, (u, v) -> u, LinkedHashMap::new);
  }

  /** Get value for a key from the map, which might be null. If absent, return default values. */
  public static <K, V> V getOrDefault(Map<K, V> map, K key, V defaultValue) {
    if (map == null) {
      return defaultValue;
    } else {
      return map.getOrDefault(key, defaultValue);
    }
  }

  /** Return true if the map is null or empty. */
  public static boolean isEmptyOrNull(Map<?, ?> map) {
    return map == null || map.isEmpty();
  }
}
