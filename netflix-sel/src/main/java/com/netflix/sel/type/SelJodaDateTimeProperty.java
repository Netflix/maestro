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
import java.util.Locale;
import org.joda.time.DateTime;

/** Wrapper class to support org.joda.time.DateTime.Property. */
public final class SelJodaDateTimeProperty extends AbstractSelType {
  private DateTime.Property val;

  private SelJodaDateTimeProperty(DateTime.Property val) {
    this.val = val;
  }

  static SelJodaDateTimeProperty of(DateTime.Property p) {
    return new SelJodaDateTimeProperty(p);
  }

  @Override
  public SelTypes type() {
    return SelTypes.DATETIME_PROPERTY;
  }

  @Override
  public SelJodaDateTimeProperty assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelJodaDateTimeProperty) rhs).val;
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public DateTime.Property getInternalVal() {
    return val;
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0) {
      if ("getAsText".equals(methodName)) {
        return SelString.of(val.getAsText(Locale.US));
      } else if ("withMinimumValue".equals(methodName)) {
        return SelJodaDateTime.of(val.withMinimumValue());
      } else if ("withMaximumValue".equals(methodName)) {
        return SelJodaDateTime.of(val.withMaximumValue());
      } else if ("get".equals(methodName)) {
        return SelLong.of((long) val.get());
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
    return String.valueOf(val);
  }
}
