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
import jakarta.validation.constraints.NotBlank;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Matches a subset of a workflow's steps. Used by {@link StepSelection}. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"step_ids", "step_id_prefixes", "step_id_infixes", "step_id_suffixes"},
    alphabetic = true)
@JsonDeserialize(builder = StepSelector.StepSelectorBuilder.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class StepSelector {
  /** Matches a step id in full. */
  @Nullable private final Set<@NotBlank String> stepIds;

  /** Matches a step id that starts with any of these. */
  @Nullable private final Set<@NotBlank String> stepIdPrefixes;

  /** Matches a step id that contains any of these. */
  @Nullable private final Set<@NotBlank String> stepIdInfixes;

  /** Matches a step id that ends with any of these. */
  @Nullable private final Set<@NotBlank String> stepIdSuffixes;

  /**
   * Returns true if the step id matches any criterion. A selector with no criteria matches none.
   */
  @JsonIgnore
  public boolean matches(String stepId) {
    return (stepIds != null && stepIds.contains(stepId))
        || (stepIdPrefixes != null && stepIdPrefixes.stream().anyMatch(stepId::startsWith))
        || (stepIdInfixes != null && stepIdInfixes.stream().anyMatch(stepId::contains))
        || (stepIdSuffixes != null && stepIdSuffixes.stream().anyMatch(stepId::endsWith));
  }

  /** Whether this selector carries no criteria, in which case it matches nothing. */
  @JsonIgnore
  public boolean isEmpty() {
    return (stepIds == null || stepIds.isEmpty())
        && (stepIdPrefixes == null || stepIdPrefixes.isEmpty())
        && (stepIdInfixes == null || stepIdInfixes.isEmpty())
        && (stepIdSuffixes == null || stepIdSuffixes.isEmpty());
  }

  /**
   * Returns the criteria this selector carries, e.g. {@code ids [s1, s2], prefixes [s]}. It omits
   * the unset criteria and sorts the values, so equal selectors return identical text.
   */
  @JsonIgnore
  public String describe() {
    return Stream.of(
            Map.entry("ids", orEmpty(stepIds)),
            Map.entry("prefixes", orEmpty(stepIdPrefixes)),
            Map.entry("infixes", orEmpty(stepIdInfixes)),
            Map.entry("suffixes", orEmpty(stepIdSuffixes)))
        .filter(entry -> !entry.getValue().isEmpty())
        .map(entry -> entry.getKey() + " " + new TreeSet<>(entry.getValue()))
        .collect(Collectors.joining(", "));
  }

  private static Set<String> orEmpty(@Nullable Set<String> values) {
    return values == null ? Collections.emptySet() : values;
  }

  /** builder class for lombok and jackson. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonPOJOBuilder(withPrefix = "")
  public static class StepSelectorBuilder {}
}
