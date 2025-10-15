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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.PeriodFormat;

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
    if (EXT_FUNCTIONS.containsKey(methodName)) {
      ExtFunction extFunction = EXT_FUNCTIONS.get(methodName);
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
      DateTimeFormat.forPattern("YYYYMMdd").withZoneUTC();

  private SelString tsToDateInt(SelType ts) {
    return SelString.of(dateIntFormatter.print(SelLong.create(ts).longVal()));
  }

  private SelString incrementDateInt(SelType dateInt, SelType days) {
    String newDateInt =
        dateIntFormatter.print(
            dateIntFormatter
                .parseDateTime(SelString.create(dateInt).getInternalVal())
                .plusDays(((SelLong) days).intVal()));
    return SelString.of(newDateInt);
  }

  private SelLong dateIntToTs(SelType dateInt) {
    return SelLong.of(
        dateIntFormatter.parseDateTime(SelString.create(dateInt).getInternalVal()).getMillis());
  }

  private SelLong dateIntHourToTs(SelType... args) {
    final DateTimeFormatter dateIntHourFormatter =
        DateTimeFormat.forPattern("YYYYMMddHH")
            .withZone(DateTimeZone.forID(SelString.create(args[2]).getInternalVal()));
    return SelLong.of(
        dateIntHourFormatter
            .parseDateTime(
                SelString.create(args[0]).getInternalVal()
                    + SelString.create(args[1]).getInternalVal())
            .minusDays(SelLong.create(args[3]).intVal())
            .minusHours(SelLong.create(args[4]).intVal())
            .getMillis());
  }

  private SelString timeoutForDateTimeDeadline(SelType dateTime, SelType durationStr) {
    String timeout =
        timeoutForDateTimeDeadline(
            ((SelJodaDateTime) dateTime).getInternalVal(),
            ((SelString) durationStr).getInternalVal());
    return SelString.of(timeout);
  }

  private SelString timeoutForDateIntDeadline(SelType dateInt, SelType durationStr) {
    DateTime dateTime = dateIntFormatter.parseDateTime(SelString.create(dateInt).toString());
    String timeout =
        timeoutForDateTimeDeadline(dateTime, ((SelString) durationStr).getInternalVal());
    return SelString.of(timeout);
  }

  private String timeoutForDateTimeDeadline(DateTime dateTime, String durationStr) {
    long duration =
        PeriodFormat.wordBased().parsePeriod(durationStr).toStandardDuration().getMillis();
    long deadline = dateTime.plus(duration).getMillis();
    long remainingMillis = Math.max(deadline - DateTime.now().getMillis(), 0);
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

    DateTimeFormatter fmt = dateIntFormatter.withZone(DateTimeZone.UTC);
    DateTime d1 = fmt.parseDateTime(SelString.create(fromDate).toString());
    DateTime d2 = fmt.parseDateTime(SelString.create(toDate).toString());
    int days = Days.daysBetween(d1, d2).getDays();
    List<Integer> list = new ArrayList<>();
    int increment = Math.abs(inc);
    for (int i = 0; i < days; i += increment) {
      list.add(Integer.valueOf(d1.plusDays(i).toString("yyyyMMdd")));
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
