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

import com.netflix.sel.ext.ExtFunction;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Util class to include all Maestro predefined methods */
public final class SelUtilFunc implements SelType {

  static final SelUtilFunc INSTANCE = new SelUtilFunc();

  private static final Map<String, ExtFunction> EXT_FUNCTIONS = new ConcurrentHashMap<>();

  public static void register(String methodName, ExtFunction extFunction) {
    EXT_FUNCTIONS.put(methodName, extFunction);
  }

  private SelUtilFunc() {}

  @Override
  public SelTypes type() {
    return SelTypes.PRESET_UTIL_FUNCTION;
  }

  @Override
  public String toString() {
    return "Preset SEL Utility Function Class";
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    ExtFunction extFunction = EXT_FUNCTIONS.get(methodName);
    if (extFunction != null) {
      Object[] varargs = new Object[args.length];
      for (int i = 0; i < varargs.length; ++i) {
        varargs[i] = args[i].unbox();
      }
      return SelTypeUtil.box(extFunction.call(varargs));
    }

    if (args.length == 1) {
      if ("dateIntToTs".equals(methodName)) {
        return dateIntToTs(args[0]);
      } else if ("tsToDateInt".equals(methodName)) {
        return tsToDateInt(args[0]);
      }
    } else if (args.length == 2) {
      if ("incrementDateInt".equals(methodName)) {
        return incrementDateInt(args[0], args[1]);
      } else if ("timeoutForDateTimeDeadline".equals(methodName)) {
        return timeoutForDateTimeDeadline(args[0], args[1]);
      } else if ("timeoutForDateIntDeadline".equals(methodName)) {
        return timeoutForDateIntDeadline(args[0], args[1]);
      }
    } else if (args.length == 3) {
      if ("dateIntsBetween".equals(methodName)) {
        return dateIntsBetween(args[0], args[1], args[2]);
      } else if ("intsBetween".equals(methodName)) {
        return intsBetween(args[0], args[1], args[2]);
      }
    } else if (args.length == 5 && "dateIntHourToTs".equals(methodName)) {
      return dateIntHourToTs(args);
    }

    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  private final DateTimeFormatter dateIntFormatter =
      new DateTimeFormatterBuilder()
          .appendPattern("uuuuMMdd")
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .toFormatter()
          .withResolverStyle(java.time.format.ResolverStyle.STRICT)
          .withZone(ZoneId.of("UTC"));

  private SelString tsToDateInt(SelType ts) {
    return SelString.of(dateIntFormatter.format(ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(SelLong.create(ts).longVal()), ZoneId.of("UTC"))));
  }

  private SelString incrementDateInt(SelType dateInt, SelType days) {
    String newDateInt =
        dateIntFormatter.format(
            ZonedDateTime.parse(SelString.create(dateInt).getInternalVal(), dateIntFormatter)
                .plusDays(((SelLong) days).intVal()));
    return SelString.of(newDateInt);
  }

  private SelLong dateIntToTs(SelType dateInt) {
    return SelLong.of(
        ZonedDateTime.parse(SelString.create(dateInt).getInternalVal(), dateIntFormatter).toInstant().toEpochMilli());
  }

  private SelLong dateIntHourToTs(SelType... args) {
    final DateTimeFormatter dateIntHourFormatter =
        new DateTimeFormatterBuilder()
            .appendPattern("uuuuMMddHH")
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
            .toFormatter()
            .withResolverStyle(java.time.format.ResolverStyle.STRICT)
            .withZone(ZoneId.of(SelString.create(args[2]).getInternalVal()));
    return SelLong.of(
        ZonedDateTime.parse(
                SelString.create(args[0]).toString()
                    + String.format("%02d", SelLong.create(args[1]).intVal()), dateIntHourFormatter)
            .minusDays(SelLong.create(args[3]).intVal())
            .minusHours(SelLong.create(args[4]).intVal())
            .toInstant().toEpochMilli());
  }

  private SelString timeoutForDateTimeDeadline(SelType dateTime, SelType durationStr) {
    String timeout =
        timeoutForDateTimeDeadline(
            (ZonedDateTime) ((SelJodaDateTime) dateTime).getInternalVal(),
            ((SelString) durationStr).getInternalVal());
    return SelString.of(timeout);
  }

  private SelString timeoutForDateIntDeadline(SelType dateInt, SelType durationStr) {
    ZonedDateTime dateTime = ZonedDateTime.parse(SelString.create(dateInt).toString(), dateIntFormatter);
    String timeout =
        timeoutForDateTimeDeadline(dateTime, ((SelString) durationStr).getInternalVal());
    return SelString.of(timeout);
  }

  private long parsePeriodDuration(String durationStr) {
      long millis = 0;
      String[] parts = durationStr.split("(?<=\\D)(?=\\d)|(?<=\\d)(?=\\D)");
      for(int i = 0; i < parts.length; i+=2) {
          if (i + 1 >= parts.length) break;
          long val = Long.parseLong(parts[i].trim());
          String unit = parts[i+1].trim().toLowerCase();
          if (unit.startsWith("day")) millis += val * 24 * 60 * 60 * 1000;
          else if (unit.startsWith("hour")) millis += val * 60 * 60 * 1000;
          else if (unit.startsWith("minute")) millis += val * 60 * 1000;
          else if (unit.startsWith("second")) millis += val * 1000;
          else if (unit.startsWith("milli")) millis += val;
      }
      return millis;
  }

  private String timeoutForDateTimeDeadline(ZonedDateTime dateTime, String durationStr) {
    long duration = parsePeriodDuration(durationStr);
    long deadline = dateTime.toInstant().toEpochMilli() + duration;
    long remainingMillis = Math.max(deadline - ZonedDateTime.now(SelJodaDateTime.CLOCK).toInstant().toEpochMilli(), 0);
    return remainingMillis + " milliseconds";
  }

  private SelArray dateIntsBetween(SelType from, SelType to, SelType dd) {
    int inc = Integer.parseInt(dd.toString());
    SelType fromDate, toDate;
    if (inc > 0) {
      fromDate = from;
      toDate = to;
    } else if (inc < 0) {
      fromDate = to;
      toDate = from;
    } else {
      throw new IllegalArgumentException("Invalid incremental interval value: " + inc);
    }

    ZonedDateTime d1 = ZonedDateTime.parse(SelString.create(fromDate).toString(), dateIntFormatter);
    ZonedDateTime d2 = ZonedDateTime.parse(SelString.create(toDate).toString(), dateIntFormatter);
    int days = (int) ChronoUnit.DAYS.between(d1, d2);
    List<Integer> list = new ArrayList<>();
    int increment = Math.abs(inc);
    for (int i = 0; i < days; i += increment) {
      list.add(Integer.valueOf(d1.plusDays(i).format(DateTimeFormatter.ofPattern("yyyyMMdd"))));
    }
    if (inc < 0) {
      Collections.reverse(list);
    }
    return SelArray.of(list.toArray(new Integer[0]), SelTypes.LONG_ARRAY);
  }

  private SelArray intsBetween(SelType from, SelType to, SelType dd) {
    int d1 = SelLong.create(from).intVal();
    int d2 = SelLong.create(to).intVal();
    int inc = SelLong.create(dd).intVal();
    List<Integer> list = new ArrayList<>();
    if (inc > 0) {
      for (int i = d1; i < d2; i += inc) {
        list.add(i);
      }
    } else {
      for (int i = d1; i > d2; i += inc) {
        list.add(i);
      }
    }
    return SelArray.of(list.toArray(new Integer[0]), SelTypes.LONG_ARRAY);
  }
}
