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

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.ParsableLong;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.Parameter;
import java.util.Arrays;
import java.util.function.Function;
import lombok.Data;
import org.junit.BeforeClass;
import org.junit.Test;

public class DurationParserTest extends MaestroBaseTest {
  private static final Function<ParamDefinition, Parameter> paramMapper =
      paramDefinition -> {
        Parameter param = paramDefinition.toParameter();
        param.setEvaluatedResult(param.getValue());
        param.setEvaluatedTime(123L);
        return param;
      };

  @BeforeClass
  public static void init() {
    MaestroBaseTest.init();
  }

  @Data
  private static class Timeouts {
    ParsableLong timeout1;
    ParsableLong timeout2;
    ParsableLong timeout3;
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    Timeouts expected =
        loadObject("fixtures/workflows/definition/sample-timeouts.json", Timeouts.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    Timeouts actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), Timeouts.class);
    String ser2 = MAPPER.writeValueAsString(actual);
    assertEquals(expected, actual);
    assertEquals(ser1, ser2);
  }

  @Test
  public void testGetTimeoutInMillis() throws Exception {
    Timeouts timeouts =
        loadObject("fixtures/workflows/definition/sample-timeouts.json", Timeouts.class);
    assertEquals(
        12345000L, DurationParser.getTimeoutWithParamInMillis(timeouts.timeout1, paramMapper));
    assertEquals(
        87005000L, DurationParser.getTimeoutWithParamInMillis(timeouts.timeout2, paramMapper));
    assertEquals(
        123456000L, DurationParser.getTimeoutWithParamInMillis(timeouts.timeout3, paramMapper));
  }

  @Test
  public void testValidDuration() {
    assertEquals(
        7200000L,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("2 hours"), paramMapper));
    assertEquals(
        7200000L, DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("2h"), paramMapper));
    assertEquals(
        7200000L,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("2 hour"), paramMapper));
    assertEquals(
        7200000L,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("2hour"), paramMapper));
    assertEquals(
        300000L, DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("300s"), paramMapper));
    assertEquals(
        180000L,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("3 minutes"), paramMapper));
    assertEquals(
        2000L,
        DurationParser.getTimeoutWithParamInMillis(
            ParsableLong.of("2000 milliseconds"), paramMapper));
    assertEquals(
        86400000L,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("1 days"), paramMapper));
    assertEquals(
        86400000L,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("1 day"), paramMapper));
    assertEquals(
        86400000L, DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("1 d"), paramMapper));
    assertEquals(
        90061001,
        DurationParser.getTimeoutWithParamInMillis(
            ParsableLong.of("1 d 1 h 1 min 1s 1ms"), paramMapper));
    assertEquals(
        90061001,
        DurationParser.getTimeoutWithParamInMillis(
            ParsableLong.of("1d 1h 1min 1s 1ms"), paramMapper));
    assertEquals(
        660000,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("10min 1min"), paramMapper));
    assertEquals(
        60000,
        DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("abc 1min"), paramMapper));
    assertEquals(
        123000,
        DurationParser.getTimeoutWithParamInMillis(
            ParsableLong.of("${foo}"),
            paramDefinition -> {
              Parameter param = paramDefinition.toParameter();
              param.setEvaluatedResult("123");
              param.setEvaluatedTime(123L);
              return param;
            }));
    assertEquals(
        1800000,
        DurationParser.getTimeoutWithParamInMillis(
            ParsableLong.of("${foo}"),
            paramDefinition -> {
              Parameter param = paramDefinition.toParameter();
              param.setEvaluatedResult("30min");
              param.setEvaluatedTime(123L);
              return param;
            }));
    assertEquals(
        3600000, DurationParser.getTimeoutWithParamInMillis(ParsableLong.of("3600"), paramMapper));
  }

  @Test
  public void testInvalidDuration() {
    for (String s : Arrays.asList("dfsd", "", "0", "-100", "10368000000L", "min", "day", "hour"))
      AssertHelper.assertThrows(
          "those are invalid cases",
          IllegalArgumentException.class,
          "cannot be non-positive or more than system limit: 120 days",
          () -> DurationParser.getTimeoutWithParamInMillis(ParsableLong.of(s), paramMapper));
  }
}
