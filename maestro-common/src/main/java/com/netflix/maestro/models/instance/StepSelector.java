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
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"step_ids", "step_id_prefixes", "step_id_infixes", "step_id_postfixes"},
    alphabetic = true)
@JsonDeserialize(builder = StepSelector.StepSelectorBuilder.class)
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
public class StepSelector {
  /** Exact step ids. */
  @Nullable private final Set<@NotBlank String> stepIds;

  /** Step ids starting with any of these. */
  @Nullable private final Set<@NotBlank String> stepIdPrefixes;

  /** Step ids containing any of these. */
  @Nullable private final Set<@NotBlank String> stepIdInfixes;

  /** Step ids ending with any of these. */
  @Nullable private final Set<@NotBlank String> stepIdPostfixes;

  /** Whether the step id matches any criterion here. An empty selector matches nothing. */
  @JsonIgnore
  public boolean matches(String stepId) {
    return (stepIds != null && stepIds.contains(stepId))
        || (stepIdPrefixes != null && stepIdPrefixes.stream().anyMatch(stepId::startsWith))
        || (stepIdInfixes != null && stepIdInfixes.stream().anyMatch(stepId::contains))
        || (stepIdPostfixes != null && stepIdPostfixes.stream().anyMatch(stepId::endsWith));
  }

  /** Whether this selector carries no criteria, in which case it matches nothing. */
  @JsonIgnore
  public boolean isEmpty() {
    return (stepIds == null || stepIds.isEmpty())
        && (stepIdPrefixes == null || stepIdPrefixes.isEmpty())
        && (stepIdInfixes == null || stepIdInfixes.isEmpty())
        && (stepIdPostfixes == null || stepIdPostfixes.isEmpty());
  }

  /**
   * Lists the criteria this selector actually carries, e.g. {@code ids [a, b], prefixes [load_]},
   * so it can be shown to users. Criteria that are unset are left out rather than rendered as null,
   * and values are sorted so the text is stable.
   */
  @Override
  public String toString() {
    return Stream.of(
            Map.entry("ids", orEmpty(stepIds)),
            Map.entry("prefixes", orEmpty(stepIdPrefixes)),
            Map.entry("infixes", orEmpty(stepIdInfixes)),
            Map.entry("postfixes", orEmpty(stepIdPostfixes)))
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
