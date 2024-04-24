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
package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {
      "page_info",
      "total_count",
      "elements",
    },
    alphabetic = true)
@Getter
@JsonDeserialize(builder = PaginationResult.PaginationResultBuilder.class)
@Builder
@EqualsAndHashCode
@ToString
public class PaginationResult<T> {

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPropertyOrder(
      value = {
        "start_cursor",
        "end_cursor",
        "has_previous_page",
        "has_next_page",
        "start_index",
      },
      alphabetic = true)
  @Getter
  @JsonDeserialize(builder = PageInfo.PageInfoBuilder.class)
  @Builder
  @EqualsAndHashCode
  @ToString
  public static class PageInfo {
    private final String startCursor;
    private final String endCursor;
    private final boolean hasPreviousPage;
    private final boolean hasNextPage;
    private final Long startIndex;

    /** builder class for lombok and jackson. */
    @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
    @JsonPOJOBuilder(withPrefix = "")
    public static final class PageInfoBuilder {}
  }

  @NotNull private final PageInfo pageInfo;
  private final long totalCount;
  @NotNull private final List<T> elements;

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static final class PaginationResultBuilder<T> {}
}
