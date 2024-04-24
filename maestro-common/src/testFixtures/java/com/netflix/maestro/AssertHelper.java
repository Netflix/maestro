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
package com.netflix.maestro;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.Assert;

/** Utility test assert helper class. */
public final class AssertHelper {
  private AssertHelper() {}

  /** Utility method to assert an exception. */
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Runnable runnable) {
    assertThrows(message, expected, containedInMessage, Executors.callable(runnable));
  }

  /** Utility method to assert an exception. */
  public static void assertThrows(
      String message,
      Class<? extends Exception> expected,
      String containedInMessage,
      Callable<?> callable) {
    try {
      callable.call();
      Assert.fail(
          String.format(
              "No expected exception (%s) was thrown with message: (%s)",
              expected.getName(), message));
    } catch (Exception actual) {
      Assert.assertEquals(message, expected, actual.getClass());
      Assert.assertTrue(
          String.format(
              "Actual exception message (%s) did not contain (%s)",
              actual.getMessage(), containedInMessage),
          actual.getMessage().contains(containedInMessage));
    }
  }
}
