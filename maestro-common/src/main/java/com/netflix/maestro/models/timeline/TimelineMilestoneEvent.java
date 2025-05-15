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
package com.netflix.maestro.models.timeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Timeline milestone event to record compute related milestones. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"timestamp", "type", "status", "milestone_type", "source_id", "info"},
    alphabetic = true)
@JsonDeserialize(builder = TimelineMilestoneEvent.TimelineMilestoneEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class TimelineMilestoneEvent implements TimelineEvent {
  private static final int MAX_INFO_LENGTH = 1000;
  private final Long timestamp;
  private final String status;
  private final MilestoneType milestoneType;
  private final String sourceId;
  private final String info;

  /** Milestone types. */
  public enum MilestoneType {
    /** CMB Job Type. */
    CMB_JOB,
    /** CMB Task type. */
    CMB_TASK
  }

  @Override
  public Type getType() {
    return Type.MILESTONE;
  }

  @Override
  public TimelineMilestoneEvent asMilestone() {
    return this;
  }

  @JsonIgnore
  @Override
  public String getMessage() {
    return status;
  }

  @Override
  public boolean isIdentical(TimelineEvent event) {
    if (event == null || event.getType() != getType()) {
      return false;
    }
    TimelineMilestoneEvent other = (TimelineMilestoneEvent) event;
    return Objects.equals(this.status, other.status)
        && Objects.equals(this.milestoneType, other.milestoneType)
        && Objects.equals(this.info, other.info)
        && Objects.equals(this.sourceId, other.sourceId);
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class TimelineMilestoneEventBuilder {}

  /** static method to generate a {@link TimelineStatusEvent}. */
  @JsonIgnore
  public static TimelineMilestoneEvent create(
      Long timestamp, String status, MilestoneType milestoneType, String sourceId, String info) {
    String trimmedInfo = info;
    if (info != null && info.length() > MAX_INFO_LENGTH) {
      trimmedInfo = info.substring(0, MAX_INFO_LENGTH);
    }
    return TimelineMilestoneEvent.builder()
        .timestamp(timestamp)
        .status(status)
        .sourceId(sourceId)
        .milestoneType(milestoneType)
        .info(trimmedInfo)
        .build();
  }
}
