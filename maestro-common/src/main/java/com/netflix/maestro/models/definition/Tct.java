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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.parameter.LongParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import lombok.Data;

/** Target completion time (TCT) configurations. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"completed_by_hour", "duration_minutes", "completed_by_ts", "tz"},
    alphabetic = true)
@Data
public class Tct {
  private static final String COMPLETED_HOUR_TCT_TS =
      "tz_dateint_formatter = DateTimeFormat.forPattern('yyyyMMdd').withZone(DateTimeZone.forID(%s));"
          + "dt = tz_dateint_formatter.parseDateTime(TARGET_RUN_DATE).plusHours(%s).minusSeconds(1);"
          + "return dt.getMillis();";
  private static final String DURATION_MINUTES_TCT_TS =
      "return new DateTime(RUN_TS).plusMinutes(%s).getMillis();";
  private static final String PARAM_NAME = "completed_by_ts";

  private Integer durationMinutes;
  private Integer completedByHour;
  private Long completedByTs;
  private String tz;

  /** Generate a parameter for completedByTs. */
  @JsonIgnore
  public LongParamDefinition getCompletedByTsParam() {
    if (completedByTs != null) {
      return ParamDefinition.buildParamDefinition(PARAM_NAME, completedByTs);
    }
    if (completedByHour != null) {
      String timeZone = tz == null ? "WORKFLOW_CRON_TIMEZONE" : String.format("'%s'", tz);
      return LongParamDefinition.builder()
          .name(PARAM_NAME)
          .expression(String.format(COMPLETED_HOUR_TCT_TS, timeZone, completedByHour))
          .build();
    }
    if (durationMinutes != null) {
      return LongParamDefinition.builder()
          .name(PARAM_NAME)
          .expression(String.format(DURATION_MINUTES_TCT_TS, durationMinutes))
          .build();
    }
    throw new MaestroInternalError(
        "Invalid TCT definition, neither of time fields is set: %s", this);
  }
}
