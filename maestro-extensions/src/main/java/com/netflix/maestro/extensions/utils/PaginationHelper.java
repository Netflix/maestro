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

import com.netflix.maestro.exceptions.MaestroValidationException;
import com.netflix.maestro.models.api.PaginationDirection;
import com.netflix.maestro.models.api.PaginationResult;
import java.util.Collections;

/** Pagination helper utilities for the extensions module. */
public final class PaginationHelper {
  private PaginationHelper() {}

  /**
   * Validates pagination parameters and returns the pagination direction. Exactly one of first or
   * last must be non-null.
   */
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

  /** Returns an empty pagination result. */
  public static <T> PaginationResult<T> buildEmptyPaginationResult() {
    return PaginationResult.<T>builder()
        .totalCount(0)
        .elements(Collections.emptyList())
        .pageInfo(PaginationResult.PageInfo.builder().build())
        .build();
  }
}
