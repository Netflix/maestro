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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/** Utility class for a java object. */
public final class ObjectHelper {
  private ObjectHelper() {}

  /**
   * Return the value or the default one if the value is null.
   *
   * @param value input value
   * @param defaultValue default value to return if the value is null
   * @param <T> value type
   * @return the value or the default one if the value is null
   */
  public static <T> T valueOrDefault(T value, T defaultValue) {
    return value == null ? defaultValue : value;
  }

  /** Return true if the collection is null or empty. */
  public static boolean isCollectionEmptyOrNull(Collection<?> collection) {
    return collection == null || collection.isEmpty();
  }

  /** Convert the string to a numeric number and return it if yes. */
  public static OptionalLong toNumeric(String input) {
    if (input == null) {
      return OptionalLong.empty();
    }
    try {
      return OptionalLong.of(Long.parseLong(input));
    } catch (NumberFormatException nfe) {
      return OptionalLong.empty();
    }
  }

  /** Convert the string to a double number and return it if yes. */
  public static OptionalDouble toDouble(String input) {
    if (input == null) {
      return OptionalDouble.empty();
    }
    try {
      return OptionalDouble.of(Double.parseDouble(input));
    } catch (NumberFormatException nfe) {
      return OptionalDouble.empty();
    }
  }

  /** Return true if the char sequence is null or empty. */
  public static boolean isNullOrEmpty(CharSequence cs) {
    return cs == null || cs.isEmpty();
  }

  /** Split a large list into a list of small batches with a size no greater than batch size. */
  public static <T> List<List<T>> partitionList(List<T> list, int batchSize) {
    if (list == null || list.isEmpty()) {
      return Collections.emptyList();
    }
    int size = list.size();
    if (size <= batchSize) {
      return Collections.singletonList(list);
    }
    List<List<T>> batches = new ArrayList<>((size + batchSize - 1) / batchSize);
    for (int i = 0; i < size; i += batchSize) {
      batches.add(list.subList(i, Math.min(size, i + batchSize)));
    }
    return batches;
  }
}
