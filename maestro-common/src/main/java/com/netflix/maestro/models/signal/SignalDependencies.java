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
package com.netflix.maestro.models.signal;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.User;
import com.netflix.maestro.models.instance.StepDependencyMatchStatus;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.List;
import java.util.Map;
import lombok.Data;

/** Summarizes all the step dependencies current match status for a given type. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {"dependencies", "info"},
    alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class SignalDependencies {
  private List<SignalDependency> dependencies;
  @Nullable private TimelineLogEvent info;

  /** Returns true if all the step dependencies have been matched or skipped. */
  @JsonIgnore
  public boolean isSatisfied() {
    return dependencies.stream().allMatch(e -> e.getStatus().isDone());
  }

  /**
   * By passes all pending step dependencies by changing their status from PENDING to SKIPPED. It
   * also records the user and timestamp info in the {@link
   * com.netflix.maestro.models.timeline.Timeline}.
   */
  public void bypass(User user, long actionTime) {
    dependencies.stream()
        .filter(v -> !v.getStatus().isDone())
        .forEach(v -> v.setStatus(StepDependencyMatchStatus.SKIPPED));

    info =
        TimelineLogEvent.builder()
            .timestamp(actionTime)
            .level(TimelineEvent.Level.INFO)
            .message("Signal step dependencies have been bypassed by user [%s]", user)
            .build();
  }

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPropertyOrder(
      value = {"name", "status", "match_params", "signal_id"},
      alphabetic = true)
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Data
  public static class SignalDependency {
    private String name;
    private StepDependencyMatchStatus status;
    @Nullable private Map<String, SignalMatchParam> matchParams;
    @Nullable private Long signalId;

    /** Create a new {@link SignalDependency} with PENDING match status. */
    public static SignalDependency initialize(String name, Map<String, SignalMatchParam> params) {
      SignalDependency dependency = new SignalDependency();
      dependency.setName(name);
      dependency.setMatchParams(params);
      return dependency;
    }

    /** Update {@link SignalDependency} status along with a signal id, i.e. sequence id. */
    public void update(Long seqId, StepDependencyMatchStatus matchStatus) {
      this.signalId = seqId;
      this.status = matchStatus;
    }
  }
}
