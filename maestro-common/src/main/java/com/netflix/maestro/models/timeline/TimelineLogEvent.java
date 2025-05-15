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

/** Timeline event to log a message. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"timestamp", "type", "level", "message"},
    alphabetic = true)
@JsonDeserialize(builder = TimelineLogEvent.TimelineLogEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class TimelineLogEvent implements TimelineEvent {
  private final Long timestamp;
  private final Level level;
  private final String message;

  @Override
  public Type getType() {
    return Type.LOG;
  }

  @Override
  public TimelineLogEvent asLog() {
    return this;
  }

  @Override
  public boolean isIdentical(TimelineEvent event) {
    if (event == null || event.getType() != getType()) {
      return false;
    }
    TimelineLogEvent other = (TimelineLogEvent) event;
    return this.level == other.level && Objects.equals(this.message, other.message);
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class TimelineLogEventBuilder {
    /** overridden build method. */
    public TimelineLogEvent build() {
      if (timestamp == null) {
        timestamp = System.currentTimeMillis();
      }
      if (level == null) {
        level = Level.INFO;
      }
      return new TimelineLogEvent(timestamp, level, message);
    }

    /** build message with arguments. */
    public TimelineLogEventBuilder message(String template, Object... args) {
      this.message = String.format(template, args);
      return this;
    }

    /** build message without arguments. */
    public TimelineLogEventBuilder message(String message) {
      this.message = message;
      return this;
    }
  }

  /** static method to generate a trace level {@link TimelineLogEvent}. */
  @JsonIgnore
  public static TimelineLogEvent trace(String template, Object... args) {
    return TimelineLogEvent.builder().level(Level.TRACE).message(template, args).build();
  }

  /** static method to generate a debug level {@link TimelineLogEvent}. */
  @JsonIgnore
  public static TimelineLogEvent debug(String template, Object... args) {
    return TimelineLogEvent.builder().level(Level.DEBUG).message(template, args).build();
  }

  /** static method to generate an info level {@link TimelineLogEvent}. */
  @JsonIgnore
  public static TimelineLogEvent info(String template, Object... args) {
    return TimelineLogEvent.builder().level(Level.INFO).message(template, args).build();
  }

  /** static method to generate a warn level {@link TimelineLogEvent}. */
  @JsonIgnore
  public static TimelineLogEvent warn(String template, Object... args) {
    return TimelineLogEvent.builder().level(Level.WARN).message(template, args).build();
  }

  /** static method to generate an error level {@link TimelineLogEvent}. */
  @JsonIgnore
  public static TimelineLogEvent error(String template, Object... args) {
    return TimelineLogEvent.builder().level(Level.ERROR).message(template, args).build();
  }
}
