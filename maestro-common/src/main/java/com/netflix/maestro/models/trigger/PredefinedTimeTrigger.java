/*
 * Copyright 2025 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 3.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-1.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.models.trigger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.validations.TimeZoneConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Locale;
import lombok.Data;
import lombok.EqualsAndHashCode;

/** Predefined Time Trigger. */
@EqualsAndHashCode(callSuper = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public class PredefinedTimeTrigger extends TimeTriggerWithJitter {
  @NotNull private PredefinedSchedule expression;
  @Valid @TimeZoneConstraint private String timezone;

  @Override
  public TimeTrigger.Type getType() {
    return Type.PREDEFINED;
  }

  @Override
  public String toString() {
    return "PredefinedTimeTrigger(expression="
        + this.getExpression()
        + ", timezone="
        + this.getTimezone()
        + (getFuzzyMaxDelay() != null ? (", fuzzyMaxDelay=" + getFuzzyMaxDelay().toString()) : "")
        + ")";
  }

  /** Supported artifact types. */
  public enum PredefinedSchedule {
    /** Hourly. */
    HOURLY("1 * * * *"),
    /** Daily. */
    DAILY("1 0 * * *"),
    /** Weekly. */
    WEEKLY("1 0 * * 0"),
    /** Monthly. */
    MONTHLY("1 0 1 * *"),
    /** Yearly. */
    YEARLY("1 0 1 1 *");

    private static final char DEFINITION_PREFIX = '@';
    private final String key;

    PredefinedSchedule(String key) {
      this.key = key;
    }

    /**
     * returns the scheduled key.
     *
     * @return the key
     */
    public String key() {
      return key;
    }

    /**
     * creates the PredefinedSchedule keys with '@' prefix.
     *
     * @return name with prefix
     */
    @JsonValue
    public String nameWithPrefix() {
      return DEFINITION_PREFIX + this.name().toLowerCase(Locale.US);
    }

    /** Static creator. */
    @JsonCreator
    public static PredefinedSchedule create(String schedule) {
      String intervalName = schedule;
      if (!schedule.isEmpty() && schedule.charAt(0) == DEFINITION_PREFIX) {
        intervalName = schedule.substring(1);
      }
      return PredefinedSchedule.valueOf(intervalName.toUpperCase(Locale.US));
    }
  }
}
