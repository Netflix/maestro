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

import java.util.function.Supplier;

/** Similar to guava Preconditions to avoid adding guava dependency. */
public final class Checks {

  /** private constructor for utility class. */
  private Checks() {}

  /**
   * Check if the condition is true. If not, throw an IllegalArgumentException.
   *
   * @param condition boolean condition to check
   * @param template error message template
   * @param args message argument
   * @throws IllegalArgumentException if condition is false
   */
  public static void checkTrue(boolean condition, String template, Object... args) {
    if (!condition) {
      throw new IllegalArgumentException(String.format(template, args));
    }
  }

  /**
   * Check if the condition is true. If not, throw an IllegalArgumentException.
   *
   * @param condition boolean condition to check
   * @param errorMessage Supplier of errorMessage
   * @throws IllegalArgumentException if condition is false
   */
  public static void checkTrue(boolean condition, Supplier<String> errorMessage) {
    if (!condition) {
      throw new IllegalArgumentException(errorMessage.get());
    }
  }

  /**
   * Check if the input is not null. If null, throw a {@link NullPointerException}.
   *
   * @param input input object to check
   * @param template error message template
   * @param args message argument
   * @param <T> input object type
   * @return input object
   * @throws NullPointerException if input is null
   */
  public static <T> T notNull(T input, String template, Object... args) {
    if (input == null) {
      throw new NullPointerException(String.format(template, args));
    }
    return input;
  }
}
