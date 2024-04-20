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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.ReadableInstant;
import org.joda.time.base.AbstractDateTime;
import org.joda.time.base.BaseDateTime;
import org.joda.time.format.DateTimeFormatter;

/** Wrapper class to support org.joda.time.DateTime. */
public final class SelJodaDateTime extends AbstractSelType {
  private DateTime val;

  private SelJodaDateTime(DateTime val) {
    this.val = val;
  }

  static SelJodaDateTime of(DateTime d) {
    return new SelJodaDateTime(d);
  }

  static SelJodaDateTime create(SelType[] args) {
    if (args.length == 0) {
      return new SelJodaDateTime(new DateTime());
    } else if (args.length == 1 && args[0].type() == SelTypes.LONG) {
      return new SelJodaDateTime(new DateTime(((SelLong) args[0]).longVal()));
    } else if (args.length == 1) {
      return new SelJodaDateTime(new DateTime(args[0].getInternalVal()));
    } else if (args.length == 2) {
      return new SelJodaDateTime(
          new DateTime(args[0].getInternalVal(), ((SelJodaDateTimeZone) args[1]).getInternalVal()));
    } else if (args.length == 8) {
      return new SelJodaDateTime(
          new DateTime(
              ((SelLong) args[0]).intVal(),
              ((SelLong) args[1]).intVal(),
              ((SelLong) args[2]).intVal(),
              ((SelLong) args[3]).intVal(),
              ((SelLong) args[4]).intVal(),
              ((SelLong) args[5]).intVal(),
              ((SelLong) args[6]).intVal(),
              ((SelJodaDateTimeZone) args[7]).getInternalVal()));
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
  public DateTime getInternalVal() {
    return val;
  }

  private static final Map<String, MethodHandle> SUPPORTED_METHODS;

  static {
    Map<String, MethodHandle> map = new HashMap<>();
    try {
      map.put(
          "toString0",
          MethodHandles.lookup()
              .findVirtual(DateTime.class, "toString", MethodType.methodType(String.class)));
      map.put(
          "toString1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "toString", MethodType.methodType(String.class, String.class)));
      map.put(
          "parse2",
          MethodHandles.lookup()
              .findStatic(
                  DateTime.class,
                  "parse",
                  MethodType.methodType(DateTime.class, String.class, DateTimeFormatter.class)));
      map.put(
          "withZone1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withZone",
                  MethodType.methodType(DateTime.class, DateTimeZone.class)));

      map.put(
          "minusYears1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusYears", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusYears1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusYears", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusMonths1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusMonths", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusMonths1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusMonths", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusWeeks1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusWeeks", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusWeeks1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusWeeks", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusDays1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusDays", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusDays1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusDays", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusHours1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusHours", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusHours1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusHours", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusMinutes1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusMinutes", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusMinutes1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusMinutes", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusSeconds1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusSeconds", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusSeconds1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusSeconds", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "minusMillis1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minusMillis", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "plusMillis1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "plusMillis", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));

      map.put(
          "isAfter1",
          MethodHandles.lookup()
              .findVirtual(
                  ReadableInstant.class,
                  "isAfter",
                  MethodType.methodType(boolean.class, ReadableInstant.class)));
      map.put(
          "isBefore1",
          MethodHandles.lookup()
              .findVirtual(
                  ReadableInstant.class,
                  "isBefore",
                  MethodType.methodType(boolean.class, ReadableInstant.class)));
      map.put(
          "isEqual1",
          MethodHandles.lookup()
              .findVirtual(
                  ReadableInstant.class,
                  "isEqual",
                  MethodType.methodType(boolean.class, ReadableInstant.class)));

      map.put(
          "monthOfYear0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "monthOfYear", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "weekyear0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "weekyear", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "weekOfWeekyear0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "weekOfWeekyear",
                  MethodType.methodType(DateTime.Property.class)));
      map.put(
          "dayOfYear0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "dayOfYear", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "dayOfMonth0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "dayOfMonth", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "dayOfWeek0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "dayOfWeek", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "hourOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "hourOfDay", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "minuteOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minuteOfDay", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "minuteOfHour0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "minuteOfHour", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "secondOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "secondOfDay", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "secondOfMinute0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "secondOfMinute",
                  MethodType.methodType(DateTime.Property.class)));
      map.put(
          "millisOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "millisOfDay", MethodType.methodType(DateTime.Property.class)));
      map.put(
          "millisOfSecond0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "millisOfSecond",
                  MethodType.methodType(DateTime.Property.class)));

      map.put(
          "withTimeAtStartOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "withTimeAtStartOfDay", MethodType.methodType(DateTime.class)));
      map.put(
          "withYear1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "withYear", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withWeekyear1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "withWeekyear", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withMonthOfYear1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withMonthOfYear",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withWeekOfWeekyear1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withWeekOfWeekyear",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withDayOfYear1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "withDayOfYear", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withDayOfMonth1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withDayOfMonth",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withDayOfWeek1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "withDayOfWeek", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withHourOfDay1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class, "withHourOfDay", MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withMinuteOfHour1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withMinuteOfHour",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withSecondOfMinute1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withSecondOfMinute",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withMillisOfSecond1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withMillisOfSecond",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));
      map.put(
          "withMillisOfDay1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "withMillisOfDay",
                  MethodType.methodType(DateTime.class, int.class))
              .asType(MethodType.methodType(DateTime.class, DateTime.class, Integer.class)));

      map.put(
          "getMillis0",
          MethodHandles.lookup()
              .findVirtual(BaseDateTime.class, "getMillis", MethodType.methodType(long.class))
              .asType(MethodType.methodType(Long.class, DateTime.class)));
      map.put(
          "getYear0",
          MethodHandles.lookup()
              .findVirtual(AbstractDateTime.class, "getYear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getHourOfDay0",
          MethodHandles.lookup()
              .findVirtual(AbstractDateTime.class, "getHourOfDay", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getWeekOfWeekyear0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getWeekOfWeekyear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getWeekyear0",
          MethodHandles.lookup()
              .findVirtual(AbstractDateTime.class, "getWeekyear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getDayOfWeek0",
          MethodHandles.lookup()
              .findVirtual(AbstractDateTime.class, "getDayOfWeek", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getDayOfMonth0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getDayOfMonth", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getDayOfYear0",
          MethodHandles.lookup()
              .findVirtual(AbstractDateTime.class, "getDayOfYear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getMillisOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getMillisOfDay", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getMillisOfSecond0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getMillisOfSecond", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getMinuteOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getMinuteOfDay", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getMinuteOfHour0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getMinuteOfHour", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getSecondOfMinute0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getSecondOfMinute", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getMonthOfYear0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getMonthOfYear", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));
      map.put(
          "getSecondOfDay0",
          MethodHandles.lookup()
              .findVirtual(
                  AbstractDateTime.class, "getSecondOfDay", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, DateTime.class)));

      map.put(
          "toDateTime1",
          MethodHandles.lookup()
              .findVirtual(
                  DateTime.class,
                  "toDateTime",
                  MethodType.methodType(DateTime.class, DateTimeZone.class)));
    } catch (Exception ex) {
      throw new RuntimeException("Initialization failure in DateTime static block.", ex);
    }

    SUPPORTED_METHODS = Collections.unmodifiableMap(map);
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    methodName += args.length;
    if (SUPPORTED_METHODS.containsKey(methodName)) {
      return SelTypeUtil.callJavaMethod(val, args, SUPPORTED_METHODS.get(methodName), methodName);
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
