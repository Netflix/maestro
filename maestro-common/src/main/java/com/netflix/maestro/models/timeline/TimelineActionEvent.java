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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.netflix.maestro.models.definition.User;
import java.util.Objects;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Timeline event to log a message. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"timestamp", "type", "action", "author", "message", "reason", "info"},
    alphabetic = true)
@JsonDeserialize(builder = TimelineActionEvent.TimelineActionEventBuilder.class)
@Builder
@Getter
@ToString
@EqualsAndHashCode
public class TimelineActionEvent implements TimelineEvent {
  private final Long timestamp;
  private final String action;
  private final User author;
  private final String message;
  private final String reason;
  private final Long info; // optional numeric info field to carry extra info

  @Override
  public Type getType() {
    return Type.ACTION;
  }

  @Override
  public TimelineActionEvent asAction() {
    return this;
  }

  @Override
  public boolean isIdentical(TimelineEvent event) {
    if (event == null || event.getType() != getType()) {
      return false;
    }
    TimelineActionEvent other = (TimelineActionEvent) event;
    return Objects.equals(this.action, other.action)
        && Objects.equals(this.author, other.author)
        && Objects.equals(this.message, other.message)
        && Objects.equals(this.reason, other.reason)
        && Objects.equals(this.info, other.info);
  }

  /** builder class for lombok and jackson. */
  @JsonPOJOBuilder(withPrefix = "")
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public static final class TimelineActionEventBuilder {
    /** overridden build method. */
    public TimelineActionEvent build() {
      if (timestamp == null) {
        timestamp = System.currentTimeMillis();
      }
      return new TimelineActionEvent(timestamp, action, author, message, reason, info);
    }

    /** build action with an enum object. */
    public TimelineActionEventBuilder action(Enum<?> enumValue) {
      this.action = enumValue.name();
      return this;
    }

    /** build action with a string object. */
    public TimelineActionEventBuilder action(String action) {
      this.action = action;
      return this;
    }

    /** build message with arguments. */
    public TimelineActionEventBuilder message(String template, Object... args) {
      this.message = String.format(template, args);
      return this;
    }

    /** build message without arguments. */
    public TimelineActionEventBuilder message(String message) {
      this.message = message;
      return this;
    }

    /** build reason with arguments. */
    public TimelineActionEventBuilder reason(String template, Object... args) {
      this.reason = String.format(template, args);
      return this;
    }

    /** build reason without arguments. */
    public TimelineActionEventBuilder reason(String message) {
      this.reason = message;
      return this;
    }
  }
}
