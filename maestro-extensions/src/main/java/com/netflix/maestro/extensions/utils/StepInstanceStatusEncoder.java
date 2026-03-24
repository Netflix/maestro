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
package com.netflix.maestro.extensions.utils;

import com.netflix.maestro.models.instance.StepInstance.Status;
import java.util.EnumMap;
import java.util.Map;

public final class StepInstanceStatusEncoder {
  private static final Map<Status, Long> ENCODED_MAP = createMap();
  private static final Map<Status, Long> PRIORITY_MAP = createPriorityMap();

  private StepInstanceStatusEncoder() {}

  public static long encode(Status stepInstanceStatus) {
    if (ENCODED_MAP.containsKey(stepInstanceStatus)) {
      return ENCODED_MAP.get(stepInstanceStatus);
    }
    throw new UnsupportedOperationException(
        String.format("StepInstance status %s is not explicitly encoded", stepInstanceStatus));
  }

  public static long getPriority(Status stepInstanceStatus) {
    if (PRIORITY_MAP.containsKey(stepInstanceStatus)) {
      return PRIORITY_MAP.get(stepInstanceStatus);
    }
    throw new UnsupportedOperationException(
        String.format("StepInstance status %s has no priority defined", stepInstanceStatus));
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private static Map<Status, Long> createPriorityMap() {
    Map<Status, Long> priorityMap = new EnumMap<>(Status.class);
    // 0 is reserved for unknown until the data is backfilled.
    priorityMap.put(Status.SUCCEEDED, 10L);
    priorityMap.put(Status.UNSATISFIED, 20L);
    priorityMap.put(Status.NOT_CREATED, 30L);
    priorityMap.put(Status.DISABLED, 40L);
    priorityMap.put(Status.SKIPPED, 50L);
    priorityMap.put(Status.FINISHING, 60L);
    priorityMap.put(Status.STARTING, 70L);
    priorityMap.put(Status.INITIALIZED, 80L);
    priorityMap.put(Status.CREATED, 90L);
    priorityMap.put(Status.COMPLETED_WITH_ERROR, 100L);
    priorityMap.put(Status.STOPPED, 110L);
    priorityMap.put(Status.RUNNING, 120L);
    priorityMap.put(Status.WAITING_FOR_PERMITS, 130L);
    priorityMap.put(Status.EVALUATING_PARAMS, 140L);
    priorityMap.put(Status.WAITING_FOR_SIGNALS, 150L);
    priorityMap.put(Status.TIMED_OUT, 160L);
    priorityMap.put(Status.INTERNALLY_FAILED, 170L);
    priorityMap.put(Status.FATALLY_FAILED, 180L);
    priorityMap.put(Status.PLATFORM_FAILED, 190L);
    priorityMap.put(Status.USER_FAILED, 200L);
    priorityMap.put(Status.TIMEOUT_FAILED, 201L);
    priorityMap.put(Status.PAUSED, 210L);
    return priorityMap;
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private static Map<Status, Long> createMap() {
    Map<Status, Long> statusEncodeMap = new EnumMap<>(Status.class);
    // For non-terminal step status below we leave gaps in the encoding to accommodate future
    // non-terminal states
    statusEncodeMap.put(Status.NOT_CREATED, 0L);
    statusEncodeMap.put(Status.CREATED, 10L);
    statusEncodeMap.put(Status.INITIALIZED, 20L);
    statusEncodeMap.put(Status.PAUSED, 30L);
    statusEncodeMap.put(Status.WAITING_FOR_SIGNALS, 40L);
    statusEncodeMap.put(Status.EVALUATING_PARAMS, 50L);
    statusEncodeMap.put(Status.WAITING_FOR_PERMITS, 60L);
    statusEncodeMap.put(Status.STARTING, 70L);
    statusEncodeMap.put(Status.RUNNING, 80L);
    statusEncodeMap.put(Status.FINISHING, 90L);
    // For terminal step status below we don't leave gaps as ordering among terminal states doesn't
    // matter as the foreach flattened step attempt sequence is incremented when transitioning to a
    // non-terminal state from a terminal state as we prioritize attempt sequence for picking latest
    // step attempt sequence.
    statusEncodeMap.put(Status.DISABLED, 100L);
    statusEncodeMap.put(Status.UNSATISFIED, 101L);
    statusEncodeMap.put(Status.SKIPPED, 102L);
    statusEncodeMap.put(Status.SUCCEEDED, 103L);
    statusEncodeMap.put(Status.COMPLETED_WITH_ERROR, 104L);
    statusEncodeMap.put(Status.USER_FAILED, 105L);
    statusEncodeMap.put(Status.PLATFORM_FAILED, 106L);
    statusEncodeMap.put(Status.FATALLY_FAILED, 107L);
    statusEncodeMap.put(Status.INTERNALLY_FAILED, 108L);
    statusEncodeMap.put(Status.STOPPED, 109L);
    statusEncodeMap.put(Status.TIMED_OUT, 110L);
    statusEncodeMap.put(Status.TIMEOUT_FAILED, 111L);
    return statusEncodeMap;
  }
}
