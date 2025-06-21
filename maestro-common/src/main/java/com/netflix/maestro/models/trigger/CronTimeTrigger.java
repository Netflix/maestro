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
package com.netflix.maestro.models.trigger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.validations.CronConstraint;
import com.netflix.maestro.validations.TimeZoneConstraint;
import jakarta.validation.Valid;
import lombok.Data;
import lombok.EqualsAndHashCode;

/** Cron based time trigger. */
@EqualsAndHashCode(callSuper = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public class CronTimeTrigger extends TimeTriggerWithJitter {
  @Valid @CronConstraint private String cron;
  @Valid @TimeZoneConstraint private String timezone;

  @Override
  public TimeTrigger.Type getType() {
    return Type.CRON;
  }

  @Override
  public String toString() {
    return "CronTimeTrigger(cron="
        + this.getCron()
        + ", timezone="
        + this.getTimezone()
        + (getFuzzyMaxDelay() != null ? (", fuzzyMaxDelay=" + getFuzzyMaxDelay()) : "")
        + ")";
  }
}
