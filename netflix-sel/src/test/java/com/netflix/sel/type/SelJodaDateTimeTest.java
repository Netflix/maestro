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
package com.netflix.sel.type;

import static org.junit.Assert.*;

import com.netflix.sel.visitor.SelOp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SelJodaDateTimeTest {
  private SelJodaDateTime one;
  private SelJodaDateTime another;

  @Before
  public void setUp() throws Exception {
    SelJodaDateTime.CLOCK = Clock.fixed(Instant.ofEpochMilli(12345L), ZoneId.of("UTC"));
    one = SelJodaDateTime.of(ZonedDateTime.ofInstant(Instant.ofEpochMilli(12345L), ZoneId.of("UTC")));
    another = SelJodaDateTime.of(ZonedDateTime.parse("2019-01-01T00:00:00Z"));
  }

  @After
  public void tearDown() throws Exception {
    SelJodaDateTime.CLOCK = Clock.systemDefaultZone();
  }

  @Test
  public void assignOps() {
    one.assignOps(SelOp.ASSIGN, another);
    assertEquals("DATETIME: 2019-01-01T00:00Z", one.type() + ": " + one);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAssignType() {
    one.assignOps(SelOp.ASSIGN, SelString.of("foo"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOps() {
    one.assignOps(SelOp.ADD_ASSIGN, SelString.of("foo"));
  }

  @Test
  public void testIntArgCalls() {
    String[] methods =
        new String[] {
          "minusYears",
          "plusYears",
          "minusMonths",
          "plusMonths",
          "minusWeeks",
          "plusWeeks",
          "minusDays",
          "plusDays",
          "minusHours",
          "plusHours",
          "minusMinutes",
          "plusMinutes",
          "minusSeconds",
          "plusSeconds",
          "minusMillis",
          "plusMillis",
          "withYear",
          "withWeekyear",
          "withMonthOfYear",
          "withWeekOfWeekyear",
          "withDayOfYear",
          "withDayOfMonth",
          "withDayOfWeek",
          "withHourOfDay",
          "withMinuteOfHour",
          "withSecondOfMinute",
          "withMillisOfSecond",
          "withMillisOfDay"
        };

    String[] results =
        new String[] {
          "DATETIME: 1969-01-01T00:00:12.345Z",
          "DATETIME: 1971-01-01T00:00:12.345Z",
          "DATETIME: 1969-12-01T00:00:12.345Z",
          "DATETIME: 1970-02-01T00:00:12.345Z",
          "DATETIME: 1969-12-25T00:00:12.345Z",
          "DATETIME: 1970-01-08T00:00:12.345Z",
          "DATETIME: 1969-12-31T00:00:12.345Z",
          "DATETIME: 1970-01-02T00:00:12.345Z",
          "DATETIME: 1969-12-31T23:00:12.345Z",
          "DATETIME: 1970-01-01T01:00:12.345Z",
          "DATETIME: 1969-12-31T23:59:12.345Z",
          "DATETIME: 1970-01-01T00:01:12.345Z",
          "DATETIME: 1970-01-01T00:00:11.345Z",
          "DATETIME: 1970-01-01T00:00:13.345Z",
          "DATETIME: 1970-01-01T00:00:12.344Z",
          "DATETIME: 1970-01-01T00:00:12.346Z",
          "DATETIME: 0001-01-01T00:00:12.345Z",
          "DATETIME: 0001-01-04T00:00:12.345Z",
          "DATETIME: 1970-01-01T00:00:12.345Z",
          "DATETIME: 1970-01-01T00:00:12.345Z",
          "DATETIME: 1970-01-01T00:00:12.345Z",
          "DATETIME: 1970-01-01T00:00:12.345Z",
          "DATETIME: 1969-12-29T00:00:12.345Z",
          "DATETIME: 1970-01-01T01:00:12.345Z",
          "DATETIME: 1970-01-01T00:01:12.345Z",
          "DATETIME: 1970-01-01T00:00:01.345Z",
          "DATETIME: 1970-01-01T00:00:12.001Z",
          "DATETIME: 1970-01-01T00:00:00.001Z"
        };

    for (int i = 0; i < methods.length; ++i) {
      SelType res = one.call(methods[i], new SelType[] {SelLong.of(1)});
      assertEquals(results[i], res.type() + ": " + res.toString().replace("Z[UTC]", "Z")); // adjusting for formatting
    }
  }

  @Test
  public void testOneArgCalls() {
    SelType res = one.call("toString", new SelType[] {SelString.of("yyyy")});
    assertEquals("STRING: 1970", res.type() + ": " + res);

    res =
        SelJodaDateTime.of(null)
            .call(
                "parse",
                new SelType[] {
                  SelString.of("20190101"),
                  SelJodaDateTimeFormatter.of(null).call("forPattern", new SelType[] {SelString.of("yyyyMMdd")})
                });
    assertEquals("DATETIME: 2019-01-01T00:00Z[UTC]", res.type() + ": " + res);

    res = one.call("withZone", new SelType[] {SelJodaDateTimeZone.of(ZoneId.of("UTC"))});
    assertEquals("DATETIME: 1970-01-01T00:00:12.345Z[UTC]", res.type() + ": " + res);

    res = one.call("isAfter", new SelType[] {another});
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = one.call("isBefore", new SelType[] {another});
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = one.call("isEqual", new SelType[] {another});
    assertEquals("BOOLEAN: false", res.type() + ": " + res);

    res = one.call("withTimeAtStartOfDay", new SelType[] {});
    assertEquals("DATETIME: 1970-01-01T00:00Z[UTC]", res.type() + ": " + res);

    res = one.call("toDateTime", new SelType[] {SelJodaDateTimeZone.of(ZoneId.of("UTC"))});
    assertEquals("DATETIME: 1970-01-01T00:00:12.345Z[UTC]", res.type() + ": " + res);
  }

  @Test
  public void testNoArgCalls() {
    String[] methods =
        new String[] {
          "monthOfYear",
          "weekyear",
          "weekOfWeekyear",
          "dayOfYear",
          "dayOfMonth",
          "dayOfWeek",
          "hourOfDay",
          "minuteOfDay",
          "minuteOfHour",
          "secondOfDay",
          "secondOfMinute",
          "millisOfDay",
          "millisOfSecond",
          "getMillis",
          "getYear",
          "getHourOfDay",
          "getWeekOfWeekyear",
          "getWeekyear",
          "getDayOfWeek",
          "getDayOfMonth",
          "getDayOfYear",
          "getMillisOfDay",
          "getMillisOfSecond",
          "getMinuteOfDay",
          "getMinuteOfHour",
          "getSecondOfMinute",
          "getMonthOfYear",
          "getSecondOfDay",
          "toString"
        };

    String[] results =
        new String[] {
          "DATETIME_PROPERTY: Property[MonthOfYear]",
          "DATETIME_PROPERTY: Property[WeekBasedYear]",
          "DATETIME_PROPERTY: Property[WeekOfWeekBasedYear]",
          "DATETIME_PROPERTY: Property[DayOfYear]",
          "DATETIME_PROPERTY: Property[DayOfMonth]",
          "DATETIME_PROPERTY: Property[DayOfWeek]",
          "DATETIME_PROPERTY: Property[HourOfDay]",
          "DATETIME_PROPERTY: Property[MinuteOfDay]",
          "DATETIME_PROPERTY: Property[MinuteOfHour]",
          "DATETIME_PROPERTY: Property[SecondOfDay]",
          "DATETIME_PROPERTY: Property[SecondOfMinute]",
          "DATETIME_PROPERTY: Property[MilliOfDay]",
          "DATETIME_PROPERTY: Property[MilliOfSecond]",
          "LONG: 12345",
          "LONG: 1970",
          "LONG: 0",
          "LONG: 1",
          "LONG: 1970",
          "LONG: 4",
          "LONG: 1",
          "LONG: 1",
          "LONG: 12345",
          "LONG: 345",
          "LONG: 0",
          "LONG: 0",
          "LONG: 12",
          "LONG: 1",
          "LONG: 12",
          "STRING: 1970-01-01T00:00:12.345Z[UTC]"
        };
    for (int i = 0; i < methods.length; ++i) {
      SelType res = one.call(methods[i], new SelType[] {});
      assertEquals(results[i], res.type() + ": " + res);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCallArg() {
    one.call("minusYears", new SelType[] {SelType.NULL});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallMethod() {
    one.call("invalid", new SelType[] {});
  }
}
