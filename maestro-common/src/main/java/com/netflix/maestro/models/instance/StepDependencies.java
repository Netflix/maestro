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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.utils.Checks;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;

/** Summarizes all the step dependencies current match status for a given type. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {"info", "type", "statuses"},
    alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class StepDependencies {
  private final StepDependencyType type;
  private final List<StepDependencyStatus> statuses;
  @Nullable private TimelineLogEvent info;

  public StepDependencies(StepDependencyType type, List<MapParameter> params) {
    this(type, initStatuses(params), null);
  }

  @JsonCreator
  public StepDependencies(
      @JsonProperty("type") StepDependencyType type,
      @JsonProperty("statuses") List<StepDependencyStatus> statuses,
      @JsonProperty("info") TimelineLogEvent info) {
    Checks.notNull(type, "StepDependencyType is mandatory");
    Checks.checkTrue(statuses != null, "status cannot be null");
    this.type = type;
    this.statuses = statuses;
    this.info = info;
  }

  private static List<StepDependencyStatus> initStatuses(List<MapParameter> params) {
    // validate if name doesn't exist
    return params.stream()
        .map(StepDependencyStatus::createWithPendingStatus)
        .collect(Collectors.toList());
  }

  /** Returns true if all the step dependencies have been matched or skipped. */
  @JsonIgnore
  public boolean isSatisfied() {
    return statuses.stream().noneMatch(e -> e.getStatus() == StepDependencyMatchStatus.PENDING);
  }

  /**
   * By passes all pending step dependencies by changing their status from PENDING to SKIPPED. It
   * also records the user and timestamp info in the {@link
   * com.netflix.maestro.models.timeline.Timeline}.
   */
  public void bypass(User user, long actionTime) {
    statuses.stream()
        .filter(v -> v.getStatus() == StepDependencyMatchStatus.PENDING)
        .forEach(v -> v.setStatus(StepDependencyMatchStatus.SKIPPED));

    info =
        TimelineLogEvent.builder()
            .timestamp(actionTime)
            .level(TimelineEvent.Level.INFO)
            .message("Step dependencies have been bypassed by user %s", user)
            .build();
  }
}
