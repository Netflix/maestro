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
import org.joda.time.Days;

/** Wrapper class to support org.joda.time.Days. */
public final class SelJodaDateTimeDays extends AbstractSelType {
  private Days val;

  private SelJodaDateTimeDays(Days val) {
    this.val = val;
  }

  static SelJodaDateTimeDays of(Days d) {
    return new SelJodaDateTimeDays(d);
  }

  @Override
  public SelTypes type() {
    return SelTypes.DATETIME_DAYS;
  }

  @Override
  public SelJodaDateTimeDays assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelJodaDateTimeDays) rhs).val;
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public Days getInternalVal() {
    return val;
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0 && "getDays".equals(methodName)) {
      return SelLong.of((long) val.getDays());
    } else if (args.length == 2 && "daysBetween".equals(methodName)) {
      return new SelJodaDateTimeDays(
          Days.daysBetween(
              ((SelJodaDateTime) args[0]).getInternalVal(),
              ((SelJodaDateTime) args[1]).getInternalVal()));
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
