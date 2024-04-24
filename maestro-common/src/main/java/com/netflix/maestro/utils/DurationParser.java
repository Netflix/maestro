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
package com.netflix.maestro.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Duration;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A parser to parse the duration in string to the duration in milliseconds. */
public final class DurationParser {

  /** Private constructor for utility class. * */
  private DurationParser() {}

  private static final String PARAM_NAME = "duration_str";

  private static final Pattern SCALA_DURATION_REGEX =
      Pattern.compile(
          "((?<DAYS>\\d+?)(d| d| days|days| day|day))?"
              + "((?<HOURS>\\d+?)(h| h| hours|hours| hour|hour))?"
              + "((?<MINUTES>\\d+?)(min| min| minutes|minutes| minute|minute))?"
              + "((?<SECONDS>\\d+?)(s| s|sec| sec| seconds|seconds| second|second))?"
              + "((?<MILLISECONDS>\\d+?)(ms| ms| milliseconds|milliseconds| millisecond|millisecond))?");
  private static final TimeUnit[] TIME_UNITS =
      new TimeUnit[] {
        TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS
      };

  /** parses the scala duration type and returns timeout duration in milliseconds. */
  private static long parseDuration(String duration) {
    Matcher m = SCALA_DURATION_REGEX.matcher(duration.toLowerCase(Locale.ROOT));
    long timeout = 0;
    while (m.find()) {
      for (TimeUnit unit : TIME_UNITS) {
        long t = Checks.toNumeric(m.group(unit.name())).orElse(0L);
        timeout += unit.toMillis(t);
      }
    }
    return timeout;
  }

  public static long getDurationInMillis(Duration duration) {
    JsonNode value = duration.getValue();
    if (value.isNumber()) {
      return value.asLong();
    } else {
      return parseDuration(value.asText());
    }
  }

  public static long getDurationWithParamInMillis(
      Duration duration, Function<ParamDefinition, Parameter> paramParser) {
    JsonNode value = duration.getValue();
    long timeout;
    if (value.isNumber()) {
      timeout = TimeUnit.SECONDS.toMillis(value.asLong());
    } else {
      ParamDefinition paramDef = ParamDefinition.buildParamDefinition(PARAM_NAME, value.asText());
      String durationParam = paramParser.apply(paramDef).asString();
      timeout =
          Checks.toNumeric(durationParam)
              .map(TimeUnit.SECONDS::toMillis)
              .orElseGet(() -> DurationParser.parseDuration(durationParam));
    }
    Checks.checkTrue(
        timeout > 0 && timeout <= Constants.MAX_TIME_OUT_LIMIT_IN_MILLIS,
        "timeout [%s ms]/[%s] cannot be non-positive or more than system limit: %s days",
        timeout,
        value,
        TimeUnit.MILLISECONDS.toDays(Constants.MAX_TIME_OUT_LIMIT_IN_MILLIS));
    return timeout;
  }

  /** checks if given `duration` string matches scala's duration type. */
  public static boolean validate(String duration) {
    Matcher m = SCALA_DURATION_REGEX.matcher(duration.toLowerCase(Locale.ROOT));
    return m.matches();
  }
}
