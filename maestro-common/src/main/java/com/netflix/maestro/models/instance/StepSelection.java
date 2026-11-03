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
import com.netflix.maestro.utils.Checks;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Chooses which steps of a run actually execute. A step runs when {@code include} matches it and
 * {@code exclude} does not; every other step is marked {@link StepInstance.Status#SKIPPED} while
 * the DAG itself is left intact, so successors still evaluate their conditions against it.
 *
 * <p>With only {@code include} set, the run skips every step it does not match. With only {@code
 * exclude} set, the run skips only the steps it matches. With both set, {@code exclude} wins. Each
 * selector that is set carries at least one criterion.
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
public class StepSelection {
  /** Steps to run. When null, only {@link #exclude} skips steps. */
  @Nullable @Valid private final StepSelector include;

  /** Steps to skip. This overrides {@link #include}. */
  @Nullable @Valid private final StepSelector exclude;

  StepSelection(@Nullable StepSelector include, @Nullable StepSelector exclude) {
    Checks.checkTrue(
        include != null || exclude != null, "Step selection must set include or exclude or both");
    this.include = include;
    this.exclude = exclude;
  }

  /** Whether the given step id should be skipped under this selection. */
  @JsonIgnore
  public boolean isSkipped(String stepId) {
    boolean included = include == null || include.matches(stepId);
    boolean excluded = exclude != null && exclude.matches(stepId);
    return !included || excluded;
  }

  /**
   * Returns a sentence for the run timeline, e.g. {@code excludes steps matching ids [s1]}. It
   * names only the criteria that are set.
   */
  @JsonIgnore
  public String describe() {
    List<String> clauses = new ArrayList<>();
    if (include != null) {
      clauses.add("includes only steps matching " + include.describe());
    }
    if (exclude != null) {
      clauses.add("excludes steps matching " + exclude.describe());
    }
    return String.join(", and ", clauses);
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static class StepSelectionBuilder {}
}
