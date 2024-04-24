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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.instance.StepInstance;
import com.netflix.maestro.models.instance.WorkflowInstance;
import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Timeline event to record a status change event. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"timestamp", "type", "status"},
    alphabetic = true)
@JsonDeserialize(builder = TimelineStatusEvent.TimelineStatusEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class TimelineStatusEvent implements TimelineEvent {
  private final Long timestamp;
  private final String status;

  @Override
  public Type getType() {
    return Type.STATUS;
  }

  @Override
  public TimelineStatusEvent asStatus() {
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
    TimelineStatusEvent other = (TimelineStatusEvent) event;
    return Objects.equals(this.status, other.status);
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  public static final class TimelineStatusEventBuilder {}

  /** Get workflow instance status. */
  @JsonIgnore
  public WorkflowInstance.Status getWorkflowStatus() {
    return WorkflowInstance.Status.create(this.status);
  }

  /** Get step instance status. */
  @JsonIgnore
  public StepInstance.Status getStepStatus() {
    return StepInstance.Status.create(this.status);
  }

  /** static method to generate a {@link TimelineStatusEvent}. */
  @JsonIgnore
  public static TimelineStatusEvent create(Long timestamp, String status) {
    return TimelineStatusEvent.builder().timestamp(timestamp).status(status).build();
  }

  /** static method to generate a {@link TimelineStatusEvent}. */
  @JsonIgnore
  public static TimelineStatusEvent create(Long timestamp, WorkflowInstance.Status status) {
    return TimelineStatusEvent.builder().timestamp(timestamp).status(status.name()).build();
  }

  /** static method to generate a {@link TimelineStatusEvent}. */
  @JsonIgnore
  public static TimelineStatusEvent create(Long timestamp, StepInstance.Status status) {
    return TimelineStatusEvent.builder().timestamp(timestamp).status(status.name()).build();
  }
}
