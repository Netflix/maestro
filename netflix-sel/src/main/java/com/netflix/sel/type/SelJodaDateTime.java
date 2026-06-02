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

import com.netflix.sel.visitor.SelOp;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Wrapper class to support java.time.ZonedDateTime. */
public final class SelJodaDateTime extends AbstractSelType {
  private ZonedDateTime val;

  static Clock CLOCK = Clock.systemDefaultZone();

  private SelJodaDateTime(ZonedDateTime val) {
    this.val = val;
  }

  static SelJodaDateTime of(ZonedDateTime d) {
    return new SelJodaDateTime(d);
  }

  static SelJodaDateTime create(SelType[] args) {
    if (args.length == 0) {
      return new SelJodaDateTime(ZonedDateTime.now(CLOCK));
    } else if (args.length == 1 && args[0].type() == SelTypes.LONG) {
      return new SelJodaDateTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(((SelLong) args[0]).longVal()), CLOCK.getZone()));
    } else if (args.length == 1) {
      return new SelJodaDateTime(ZonedDateTime.from((ZonedDateTime) args[0].getInternalVal()));
    } else if (args.length == 2) {
      if (args[0].type() == SelTypes.LONG) {
        return new SelJodaDateTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(((SelLong) args[0]).longVal()), (ZoneId) ((SelJodaDateTimeZone) args[1]).getInternalVal()));
      }
      return new SelJodaDateTime(((ZonedDateTime) args[0].getInternalVal()).withZoneSameLocal((ZoneId) ((SelJodaDateTimeZone) args[1]).getInternalVal()));
    } else if (args.length == 8) {
      return new SelJodaDateTime(
          ZonedDateTime.of(
              ((SelLong) args[0]).intVal(),
              ((SelLong) args[1]).intVal(),
              ((SelLong) args[2]).intVal(),
              ((SelLong) args[3]).intVal(),
              ((SelLong) args[4]).intVal(),
              ((SelLong) args[5]).intVal(),
              ((SelLong) args[6]).intVal() * 1000000,
              (ZoneId) ((SelJodaDateTimeZone) args[7]).getInternalVal()));
    }
    throw new IllegalArgumentException(
        "Invalid input arguments (" + Arrays.toString(args) + ") for DateTime constructor");
  }

  @Override
  public SelTypes type() {
    return SelTypes.DATETIME;
  }

  @Override
  public SelJodaDateTime assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelJodaDateTime) rhs).val;
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public ZonedDateTime getInternalVal() {
    return val;
  }

  private static final Map<String, MethodHandle> SUPPORTED_METHODS;

  static {
    Map<String, MethodHandle> map = new HashMap<>();
    try {
      map.put(
          "toString0",
          MethodHandles.lookup()
              .findVirtual(ZonedDateTime.class, "toString", MethodType.methodType(String.class)));
      map.put(
          "toString1",
          MethodHandles.lookup()
              .findVirtual(
                  SelJodaDateTime.class, "toStringWithFormat", MethodType.methodType(String.class, String.class)));
      map.put(
          "parse2",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "parse",
                  MethodType.methodType(ZonedDateTime.class, CharSequence.class, DateTimeFormatter.class)));
      map.put(
          "withZone1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class,
                  "withZoneSameInstant",
                  MethodType.methodType(ZonedDateTime.class, ZoneId.class)));

      map.put(
          "minusYears1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusYears", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusYears1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusYears", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusMonths1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusMonths", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusMonths1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusMonths", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusWeeks1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusWeeks", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusWeeks1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusWeeks", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusDays1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusDays", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusDays1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusDays", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusHours1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusHours", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusHours1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusHours", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusMinutes1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusMinutes", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusMinutes1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusMinutes", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusSeconds1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "minusSeconds", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusSeconds1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "plusSeconds", MethodType.methodType(ZonedDateTime.class, long.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "minusMillis1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "minusMillis", MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "plusMillis1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "plusMillis", MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));

      map.put(
          "isAfter1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "isAfter",
                  MethodType.methodType(boolean.class, ZonedDateTime.class, ZonedDateTime.class)));
      map.put(
          "isBefore1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "isBefore",
                  MethodType.methodType(boolean.class, ZonedDateTime.class, ZonedDateTime.class)));
      map.put(
          "isEqual1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "isEqual",
                  MethodType.methodType(boolean.class, ZonedDateTime.class, ZonedDateTime.class)));

      map.put(
          "monthOfYear0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propMonthOfYear", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "weekyear0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propWeekyear", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "weekOfWeekyear0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "propWeekOfWeekyear",
                  MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "dayOfYear0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propDayOfYear", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "dayOfMonth0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propDayOfMonth", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "dayOfWeek0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propDayOfWeek", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "hourOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propHourOfDay", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "minuteOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propMinuteOfDay", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "minuteOfHour0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propMinuteOfHour", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "secondOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propSecondOfDay", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "secondOfMinute0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "propSecondOfMinute",
                  MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "millisOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "propMillisOfDay", MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));
      map.put(
          "millisOfSecond0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "propMillisOfSecond",
                  MethodType.methodType(SelJodaDateTimeProperty.class, ZonedDateTime.class)));

      map.put(
          "withTimeAtStartOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "withTimeAtStartOfDay", MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class)));
      map.put(
          "withYear1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "withYear", MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withWeekyear1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "withWeekyear", MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withMonthOfYear1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class,
                  "withMonth",
                  MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withWeekOfWeekyear1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "withWeekOfWeekyear",
                  MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withDayOfYear1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "withDayOfYear", MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withDayOfMonth1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class,
                  "withDayOfMonth",
                  MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withDayOfWeek1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "withDayOfWeek", MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withHourOfDay1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "withHour", MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withMinuteOfHour1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class,
                  "withMinute",
                  MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withSecondOfMinute1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class,
                  "withSecond",
                  MethodType.methodType(ZonedDateTime.class, int.class))
              .asType(MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withMillisOfSecond1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "withMillisOfSecond",
                  MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));
      map.put(
          "withMillisOfDay1",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class,
                  "withMillisOfDay",
                  MethodType.methodType(ZonedDateTime.class, ZonedDateTime.class, Integer.class)));

      map.put(
          "getMillis0",
          MethodHandles.lookup()
              .findStatic(SelJodaDateTime.class, "getMillis", MethodType.methodType(long.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Long.class, ZonedDateTime.class)));
      map.put(
          "getYear0",
          MethodHandles.lookup()
              .findVirtual(ZonedDateTime.class, "getYear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getHourOfDay0",
          MethodHandles.lookup()
              .findVirtual(ZonedDateTime.class, "getHour", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getWeekOfWeekyear0",
          MethodHandles.lookup()
              .findStatic(SelJodaDateTime.class, "getWeekOfWeekyear", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getWeekyear0",
          MethodHandles.lookup()
              .findStatic(SelJodaDateTime.class, "getWeekyear", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getDayOfWeek0",
          MethodHandles.lookup()
              .findStatic(SelJodaDateTime.class, "getDayOfWeek", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getDayOfMonth0",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "getDayOfMonth", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getDayOfYear0",
          MethodHandles.lookup()
              .findVirtual(ZonedDateTime.class, "getDayOfYear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getMillisOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "getMillisOfDay", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getMillisOfSecond0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "getMillisOfSecond", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getMinuteOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "getMinuteOfDay", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getMinuteOfHour0",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "getMinute", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getSecondOfMinute0",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "getSecond", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getMonthOfYear0",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class, "getMonthValue", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));
      map.put(
          "getSecondOfDay0",
          MethodHandles.lookup()
              .findStatic(
                  SelJodaDateTime.class, "getSecondOfDay", MethodType.methodType(int.class, ZonedDateTime.class))
              .asType(MethodType.methodType(Integer.class, ZonedDateTime.class)));

      map.put(
          "toDateTime1",
          MethodHandles.lookup()
              .findVirtual(
                  ZonedDateTime.class,
                  "withZoneSameInstant",
                  MethodType.methodType(ZonedDateTime.class, ZoneId.class)));
    } catch (Exception ex) {
      throw new RuntimeException("Initialization failure in DateTime static block.", ex);
    }

    SUPPORTED_METHODS = Collections.unmodifiableMap(map);
  }

  public String toStringWithFormat(String format) {
    // Basic mapping for Joda to JavaTime formats where possible
    return val.format(DateTimeFormatter.ofPattern(format.replace("YYYY", "yyyy").replace("ww", "ww").replace("DD", "DDD")));
  }

  static SelJodaDateTimeProperty propMonthOfYear(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.MONTH_OF_YEAR); }
  static SelJodaDateTimeProperty propWeekyear(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, IsoFields.WEEK_BASED_YEAR); }
  static SelJodaDateTimeProperty propWeekOfWeekyear(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, IsoFields.WEEK_OF_WEEK_BASED_YEAR); }
  static SelJodaDateTimeProperty propDayOfYear(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.DAY_OF_YEAR); }
  static SelJodaDateTimeProperty propDayOfMonth(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.DAY_OF_MONTH); }
  static SelJodaDateTimeProperty propDayOfWeek(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.DAY_OF_WEEK); }
  static SelJodaDateTimeProperty propHourOfDay(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.HOUR_OF_DAY); }
  static SelJodaDateTimeProperty propMinuteOfDay(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.MINUTE_OF_DAY); }
  static SelJodaDateTimeProperty propMinuteOfHour(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.MINUTE_OF_HOUR); }
  static SelJodaDateTimeProperty propSecondOfDay(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.SECOND_OF_DAY); }
  static SelJodaDateTimeProperty propSecondOfMinute(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.SECOND_OF_MINUTE); }
  static SelJodaDateTimeProperty propMillisOfDay(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.MILLI_OF_DAY); }
  static SelJodaDateTimeProperty propMillisOfSecond(ZonedDateTime val) { return SelJodaDateTimeProperty.of(val, ChronoField.MILLI_OF_SECOND); }

  static ZonedDateTime withTimeAtStartOfDay(ZonedDateTime d) {
    return d.toLocalDate().atStartOfDay(d.getZone());
  }
  static ZonedDateTime minusMillis(ZonedDateTime d, Integer val) { return d.minusNanos(val * 1000000L); }
  static ZonedDateTime plusMillis(ZonedDateTime d, Integer val) { return d.plusNanos(val * 1000000L); }
  
  static boolean isAfter(ZonedDateTime a, ZonedDateTime b) { return a.toInstant().isAfter(b.toInstant()); }
  static boolean isBefore(ZonedDateTime a, ZonedDateTime b) { return a.toInstant().isBefore(b.toInstant()); }
  static boolean isEqual(ZonedDateTime a, ZonedDateTime b) { return a.toInstant().equals(b.toInstant()); }

  static ZonedDateTime parse(CharSequence text, DateTimeFormatter val) {
    java.time.temporal.TemporalAccessor parsed = val.parse(text);
    ZoneId zone = val.getZone() != null ? val.getZone() : ZoneId.of("UTC");
    try {
      zone = ZoneId.from(parsed);
    } catch (Exception e) {}
    return java.time.LocalDateTime.from(parsed).atZone(zone);
  }

  static ZonedDateTime withWeekyear(ZonedDateTime d, Integer val) { return d.with(IsoFields.WEEK_BASED_YEAR, val); }
  static ZonedDateTime withWeekOfWeekyear(ZonedDateTime d, Integer val) { return d.with(IsoFields.WEEK_OF_WEEK_BASED_YEAR, val); }
  static ZonedDateTime withDayOfWeek(ZonedDateTime d, Integer val) { return d.with(ChronoField.DAY_OF_WEEK, val); }
  static ZonedDateTime withMillisOfDay(ZonedDateTime d, Integer val) { return d.with(ChronoField.MILLI_OF_DAY, val); }
  static ZonedDateTime withMillisOfSecond(ZonedDateTime d, Integer val) { return d.withNano(val * 1000000); }

  static long getMillis(ZonedDateTime d) { return d.toInstant().toEpochMilli(); }
  static int getWeekOfWeekyear(ZonedDateTime d) { return d.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR); }
  static int getWeekyear(ZonedDateTime d) { return d.get(IsoFields.WEEK_BASED_YEAR); }
  static int getDayOfWeek(ZonedDateTime d) { return d.get(ChronoField.DAY_OF_WEEK); }
  static int getMillisOfDay(ZonedDateTime d) { return d.get(ChronoField.MILLI_OF_DAY); }
  static int getMillisOfSecond(ZonedDateTime d) { return d.get(ChronoField.MILLI_OF_SECOND); }
  static int getMinuteOfDay(ZonedDateTime d) { return d.get(ChronoField.MINUTE_OF_DAY); }
  static int getSecondOfDay(ZonedDateTime d) { return d.get(ChronoField.SECOND_OF_DAY); }

  @Override
  public SelType call(String methodName, SelType[] args) {
    String methodKey = methodName + args.length;
    if (SUPPORTED_METHODS.containsKey(methodKey)) {
      if ("toString1".equals(methodKey)) {
          return SelString.of(toStringWithFormat(((SelString)args[0]).getInternalVal()));
      }
      return SelTypeUtil.callJavaMethod(val, args, SUPPORTED_METHODS.get(methodKey), methodName);
    }

    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  @Override
  public String toString() {
    return String.valueOf(val);
  }
}
