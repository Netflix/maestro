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
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Matches a subset of a workflow's steps. Used by {@link StepSelection}. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"step_ids", "step_id_pattern"},
    alphabetic = true)
@JsonDeserialize(builder = StepSelector.StepSelectorBuilder.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class StepSelector {
  /**
   * Exact step ids. Preferred over {@link #stepIdPattern} when the ids are known, since they are
   * validated against the workflow definition and cannot be written in a way that backtracks.
   */
  @Nullable private final Set<String> stepIds;

  /**
   * Regular expression matched against a step id in full, so {@code load} matches only the step
   * named {@code load} and {@code load.*} is needed to match {@code load_users}. Needed for steps
   * whose ids are not known when the request is made, such as those inside a foreach.
   */
  @Nullable private final String stepIdPattern;

  /**
   * Whether the given step id matches this selector, by being one of {@link #stepIds} or matching
   * {@link #stepIdPattern}. An empty selector matches nothing.
   */
  @JsonIgnore
  public boolean matches(String stepId) {
    if (stepIds != null && stepIds.contains(stepId)) {
      return true;
    }
    return stepIdPattern != null && Pattern.matches(stepIdPattern, stepId);
  }

  /** Whether this selector carries no criteria, in which case it matches nothing. */
  @JsonIgnore
  public boolean isEmpty() {
    return (stepIds == null || stepIds.isEmpty()) && stepIdPattern == null;
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static class StepSelectorBuilder {}
}
