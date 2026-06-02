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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/** Wrapper class to support java.time.format.DateTimeFormatter. */
public final class SelJodaDateTimeFormatter extends AbstractSelType {
  private DateTimeFormatter val;

  private SelJodaDateTimeFormatter(DateTimeFormatter val) {
    this.val = val;
  }

  static SelJodaDateTimeFormatter of(DateTimeFormatter fmt) {
    return new SelJodaDateTimeFormatter(fmt);
  }

  @Override
  public SelTypes type() {
    return SelTypes.DATETIME_FORMATTER;
  }

  @Override
  public SelJodaDateTimeFormatter assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelJodaDateTimeFormatter) rhs).val; // direct assignment
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public DateTimeFormatter getInternalVal() {
    return val;
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 1) {
      if ("withZone".equals(methodName)) {
        return new SelJodaDateTimeFormatter(
            val.withZone((ZoneId) ((SelJodaDateTimeZone) args[0]).getInternalVal()));
      } else if ("parseDateTime".equals(methodName)) {
        switch (args[0].type()) {
          case STRING:
          case LONG:
            java.time.temporal.TemporalAccessor parsed = val.parse(args[0].toString());
            ZoneId zone = val.getZone() != null ? val.getZone() : ZoneId.of("UTC");
            try {
              zone = ZoneId.from(parsed);
            } catch (Exception e) {}
            return SelJodaDateTime.of(java.time.LocalDateTime.from(parsed).atZone(zone));
        }
      } else if ("parseMillis".equals(methodName)) {
        java.time.temporal.TemporalAccessor parsed = val.parse(((SelString) args[0]).getInternalVal());
        ZoneId zone = val.getZone() != null ? val.getZone() : ZoneId.of("UTC");
        try {
          zone = ZoneId.from(parsed);
        } catch (Exception e) {}
        return SelLong.of(java.time.LocalDateTime.from(parsed).atZone(zone).toInstant().toEpochMilli());
      } else if ("forPattern".equals(methodName)) {
        String pattern = ((SelString) args[0]).getInternalVal().replace("YYYY", "uuuu").replace("yyyy", "uuuu").replace("ww", "ww").replace("DD", "DDD");
        return new SelJodaDateTimeFormatter(
            new java.time.format.DateTimeFormatterBuilder()
                .appendPattern(pattern)
                .parseDefaulting(java.time.temporal.ChronoField.YEAR, 1970)
                .parseDefaulting(java.time.temporal.ChronoField.MONTH_OF_YEAR, 1)
                .parseDefaulting(java.time.temporal.ChronoField.DAY_OF_MONTH, 1)
                .parseDefaulting(java.time.temporal.ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(java.time.temporal.ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(java.time.temporal.ChronoField.SECOND_OF_MINUTE, 0)
                .parseDefaulting(java.time.temporal.ChronoField.MILLI_OF_SECOND, 0)
                .toFormatter(java.util.Locale.US)
                .withZone(val != null && val.getZone() != null ? val.getZone() : ZoneId.of("UTC")) // Preserve zone if any
        );
      } else if ("print".equals(methodName)) {
        switch (args[0].type()) {
          case LONG:
            return SelString.of(val.format(ZonedDateTime.ofInstant(java.time.Instant.ofEpochMilli(((SelLong) args[0]).longVal()), val.getZone() != null ? val.getZone() : ZoneId.of("UTC"))));
          case DATETIME:
            return SelString.of(val.format((ZonedDateTime) ((SelJodaDateTime) args[0]).getInternalVal()));
        }
      }
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }
}
