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

import java.time.Duration;
import java.util.Locale;
import lombok.experimental.UtilityClass;

@UtilityClass
public class DurationHelper {
  /**
   * Converts {@link Duration} to human-readable string.
   *
   * <p>Example output:
   *
   * <ul>
   *   <li>5h
   *   <li>7h 15m
   *   <li>6h 50m 15s
   *   <li>2h 5s
   *   <li>48h
   * </ul>
   *
   * Note: If the duration is > 24h, we still display in hours which is the highest grain.
   *
   * @param duration time duration
   * @return human-readable duration string
   */
  public String humanReadableFormat(Duration duration) {
    return duration
        .toString()
        .substring(2)
        .replaceAll("(\\d[HMS])(?!$)", "$1 ")
        .toLowerCase(Locale.US);
  }
}
