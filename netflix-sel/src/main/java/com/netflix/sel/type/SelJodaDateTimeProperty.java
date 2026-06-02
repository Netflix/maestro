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
import java.time.ZonedDateTime;
import java.time.format.TextStyle;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.Locale;

/** Wrapper class to support date time property. */
public final class SelJodaDateTimeProperty extends AbstractSelType {
  private ZonedDateTime zdt;
  private TemporalField field;

  private SelJodaDateTimeProperty(ZonedDateTime zdt, TemporalField field) {
    this.zdt = zdt;
    this.field = field;
  }

  static SelJodaDateTimeProperty of(ZonedDateTime zdt, TemporalField field) {
    return new SelJodaDateTimeProperty(zdt, field);
  }

  @Override
  public SelTypes type() {
    return SelTypes.DATETIME_PROPERTY;
  }

  @Override
  public SelJodaDateTimeProperty assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.zdt = ((SelJodaDateTimeProperty) rhs).zdt;
      this.field = ((SelJodaDateTimeProperty) rhs).field;
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public Object getInternalVal() {
    return this; // not directly used
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0) {
      if ("getAsText".equals(methodName)) {
        if (field == java.time.temporal.ChronoField.DAY_OF_WEEK) {
            return SelString.of(java.time.format.DateTimeFormatter.ofPattern("EEEE", Locale.US).format(zdt));
        } else if (field == java.time.temporal.ChronoField.MONTH_OF_YEAR) {
            return SelString.of(java.time.format.DateTimeFormatter.ofPattern("MMMM", Locale.US).format(zdt));
        }
        return SelString.of(Long.toString(zdt.get(field)));
      } else if ("withMinimumValue".equals(methodName)) {
        return SelJodaDateTime.of(zdt.with(field, field.range().getMinimum()));
      } else if ("withMaximumValue".equals(methodName)) {
        return SelJodaDateTime.of(zdt.with(field, field.range().getMaximum()));
      } else if ("get".equals(methodName)) {
        return SelLong.of((long) zdt.get(field));
      }
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
    return "Property[" + field + "]";
  }
}
