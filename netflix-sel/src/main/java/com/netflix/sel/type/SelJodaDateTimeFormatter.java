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
import java.util.Arrays;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Wrapper class to support org.joda.time.format.DateTimeFormatter. */
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
            val.withZone(((SelJodaDateTimeZone) args[0]).getInternalVal()));
      } else if ("parseDateTime".equals(methodName)) {
        switch (args[0].type()) {
          case STRING:
          case LONG:
            return SelJodaDateTime.of(val.parseDateTime(args[0].toString()));
        }
      } else if ("parseMillis".equals(methodName)) {
        return SelLong.of(val.parseMillis(((SelString) args[0]).getInternalVal()));
      } else if ("forPattern".equals(methodName)) {
        return new SelJodaDateTimeFormatter(
            DateTimeFormat.forPattern(((SelString) args[0]).getInternalVal()));
      } else if ("print".equals(methodName)) {
        switch (args[0].type()) {
          case LONG:
            return SelString.of(val.print(((SelLong) args[0]).longVal()));
          case DATETIME:
            return SelString.of(val.print(((SelJodaDateTime) args[0]).getInternalVal()));
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
