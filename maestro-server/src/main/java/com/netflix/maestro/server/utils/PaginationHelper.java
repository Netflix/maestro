/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.server.utils;

import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.api.PaginationDirection;
import com.netflix.maestro.models.api.PaginationResult;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Utility class for handling pagination logic. Provides methods to calculate pagination ranges and
 * build pagination results.
 */
public final class PaginationHelper {
  private PaginationHelper() {}

  public record PaginationRange(long start, long end) {}

  public static PaginationRange getPaginationRange(
      String cursor,
      PaginationDirection direction,
      long earliestWorkflowInstanceId,
      long latestWorkflowInstanceId,
      long limit) {
    long instanceIdLow; // Inclusive
    long instanceIdHigh; // Inclusive
    if (cursor == null || cursor.isEmpty()) {
      if (direction == PaginationDirection.NEXT) {
        instanceIdHigh = latestWorkflowInstanceId;
        instanceIdLow = Math.max(earliestWorkflowInstanceId, (instanceIdHigh + 1) - limit);
      } else {
        instanceIdLow = earliestWorkflowInstanceId;
        instanceIdHigh = Math.min(latestWorkflowInstanceId, (instanceIdLow - 1) + limit);
      }
    } else { // if cursor exists
      if (direction == PaginationDirection.NEXT) {
        instanceIdHigh = Long.parseLong(cursor) - 1;
        instanceIdLow = Math.max(earliestWorkflowInstanceId, (instanceIdHigh + 1) - limit);
      } else {
        instanceIdLow = Long.parseLong(cursor) + 1;
        instanceIdHigh = Math.min(latestWorkflowInstanceId, (instanceIdLow - 1) + limit);
      }
    }

    if (instanceIdLow > instanceIdHigh) {
      instanceIdLow = instanceIdHigh;
    }

    return new PaginationRange(instanceIdLow, instanceIdHigh);
  }

  public static <V> PaginationResult<V> buildPaginationResult(
      List<V> pageItems,
      long latestInstanceId,
      long earliestInstanceId,
      Function<List<V>, long[]> pageRangeParser) {
    if (pageItems == null || pageItems.isEmpty()) {
      return PaginationResult.<V>builder()
          .pageInfo(
              PaginationResult.PageInfo.builder()
                  .startCursor(null)
                  .endCursor(null)
                  .hasPreviousPage(false)
                  .hasNextPage(false)
                  .startIndex(null)
                  .build())
          .totalCount(latestInstanceId)
          .elements(pageItems)
          .build();
    }
    long[] startAndEnd = pageRangeParser.apply(pageItems);
    long pageStartIndex = startAndEnd[0];
    long pageEndIndex = startAndEnd[1];
    boolean hasNextPage = pageEndIndex > earliestInstanceId;
    boolean hasPrevPage = pageStartIndex < latestInstanceId;

    String startCursor = String.valueOf(pageStartIndex);
    String endCursor = String.valueOf(pageEndIndex);

    long globalStartIndex = (latestInstanceId + 1) - pageStartIndex; // 1-indexed (not 0-indexed)
    return PaginationResult.<V>builder()
        .pageInfo(
            PaginationResult.PageInfo.builder()
                .startCursor(startCursor)
                .endCursor(endCursor)
                .hasPreviousPage(hasPrevPage)
                .hasNextPage(hasNextPage)
                .startIndex(globalStartIndex)
                .build())
        .totalCount(latestInstanceId - earliestInstanceId + 1)
        .elements(pageItems)
        .build();
  }

  public static <T> PaginationDirection validateParamAndDeriveDirection(T first, T last) {
    if (first != null && last != null) {
      throw new MaestroValidationException(
          "Either first or last need to be provided, but not both");
    }
    if (first == null && last == null) {
      throw new MaestroValidationException(
          "Either first or last need to be provided, both cannot be null");
    }
    return (last == null) ? PaginationDirection.NEXT : PaginationDirection.PREV;
  }

  public static <V> PaginationResult<V> buildEmptyPaginationResult() {
    return buildPaginationResult(Collections.emptyList(), 0, 0, (ignored) -> new long[] {0L, 0L});
  }
}
