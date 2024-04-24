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
import com.netflix.maestro.exceptions.MaestroRuntimeException;
import com.netflix.maestro.models.error.Details;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Timeline event to record {@link Details}. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"timestamp", "type", "status", "message", "errors", "retryable"},
    alphabetic = true)
@JsonDeserialize(builder = TimelineDetailsEvent.TimelineDetailsEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class TimelineDetailsEvent implements TimelineEvent {
  private final Long timestamp;
  private final MaestroRuntimeException.Code status;
  private final String message;
  private final List<String> errors;
  private final boolean retryable;

  @Override
  public Type getType() {
    return Type.DETAILS;
  }

  @Override
  public TimelineDetailsEvent asDetails() {
    return this;
  }

  @Override
  public boolean isIdentical(TimelineEvent event) {
    if (event == null || event.getType() != getType()) {
      return false;
    }
    TimelineDetailsEvent other = (TimelineDetailsEvent) event;
    return this.status == other.status
        && Objects.equals(this.message, other.message)
        && Objects.equals(this.errors, other.errors)
        && this.retryable == other.retryable;
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  public static final class TimelineDetailsEventBuilder {
    /** overridden build method. */
    public TimelineDetailsEvent build() {
      if (timestamp == null) {
        timestamp = System.currentTimeMillis();
      }
      if (status == null) {
        status = MaestroRuntimeException.Code.INTERNAL_ERROR;
      }
      return new TimelineDetailsEvent(timestamp, status, message, errors, retryable);
    }
  }

  /** create {@link TimelineDetailsEvent} from {@link Details}. */
  @JsonIgnore
  public static TimelineDetailsEvent from(Details details) {
    return TimelineDetailsEvent.builder()
        .status(details.getStatus())
        .message(details.getMessage())
        .errors(details.getErrors())
        .retryable(details.isRetryable())
        .build();
  }
}
