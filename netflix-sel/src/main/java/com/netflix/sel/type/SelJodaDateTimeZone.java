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
import org.joda.time.DateTimeZone;

/** Wrapper class to support org.joda.time.DateTimeZone. */
public final class SelJodaDateTimeZone extends AbstractSelType {
  private DateTimeZone val;

  private SelJodaDateTimeZone(DateTimeZone val) {
    this.val = val;
  }

  static SelJodaDateTimeZone of(DateTimeZone dtz) {
    return new SelJodaDateTimeZone(dtz);
  }

  @Override
  public SelTypes type() {
    return SelTypes.DATETIME_ZONE;
  }

  @Override
  public SelJodaDateTimeZone assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelJodaDateTimeZone) rhs).val;
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public DateTimeZone getInternalVal() {
    return val;
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 1) {
      if ("forID".equals(methodName)) {
        return new SelJodaDateTimeZone(DateTimeZone.forID(((SelString) args[0]).getInternalVal()));
      } else if ("getOffset".equals(methodName)) {
        return SelLong.of((long) val.getOffset(((SelJodaDateTime) args[0]).getInternalVal()));
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
  public SelJodaDateTimeZone field(SelString field) {
    String fieldName = field.getInternalVal();
    if ("UTC".equals(fieldName)) {
      return new SelJodaDateTimeZone(DateTimeZone.UTC);
    }
    throw new UnsupportedOperationException(type() + " DO NOT support accessing field: " + field);
  }

  @Override
  public String toString() {
    return String.valueOf(val);
  }
}
