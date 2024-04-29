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
package com.netflix.maestro.engine.utils;

import java.util.Collection;

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
}
