/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.annotations.Nullable;
import jakarta.validation.Valid;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Chooses which steps of a run actually execute. A step runs when {@code include} matches it and
 * {@code exclude} does not; every other step is marked {@link StepInstance.Status#SKIPPED} while
 * the DAG itself is left intact, so successors still evaluate their conditions against it.
 *
 * <p>An unset or empty {@code include} matches every step, so specifying only {@code exclude} skips
 * just the matched steps. Specifying only {@code include} skips everything it does not match.
 * {@code exclude} always wins over {@code include}.
 *
 * <p>The selection applies at every level of a run, including the inline workflows created by
 * foreach steps and the workflows started by subworkflow steps, and is matched against the step ids
 * of whichever workflow is running.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"include", "exclude"},
    alphabetic = true)
@JsonDeserialize(builder = StepSelection.StepSelectionBuilder.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class StepSelection {
  /** Steps to run. Unset or empty means every step. */
  @Nullable @Valid private final StepSelector include;

  /** Steps to skip. Applied after {@link #include} and overrides it. */
  @Nullable @Valid private final StepSelector exclude;

  /** Whether the given step id should be skipped under this selection. */
  @JsonIgnore
  public boolean isSkipped(String stepId) {
    boolean included = include == null || include.isEmpty() || include.matches(stepId);
    boolean excluded = exclude != null && exclude.matches(stepId);
    return !included || excluded;
  }

  /** Whether this selection carries no criteria and therefore skips nothing. */
  @JsonIgnore
  public boolean isEmpty() {
    return (include == null || include.isEmpty()) && (exclude == null || exclude.isEmpty());
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static class StepSelectionBuilder {}
}
