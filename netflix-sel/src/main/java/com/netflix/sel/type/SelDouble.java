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

/** Wrapper class to support float/Float/double/Double data type. */
public final class SelDouble extends AbstractSelType {
  private double val;

  private SelDouble(double val) {
    this.val = val;
  }

  static SelDouble create(SelType[] args) {
    if (args.length == 1) {
      switch (args[0].type()) {
        case DOUBLE:
          return new SelDouble(((SelDouble) args[0]).val);
        case STRING:
          return SelDouble.of(((SelString) args[0]).getInternalVal());
      }
    }
    throw new IllegalArgumentException(
        "Invalid input arguments (" + Arrays.toString(args) + ") for Double constructor");
  }

  public static SelDouble of(String s) {
    return new SelDouble(Double.parseDouble(s));
  }

  static SelDouble of(double s) {
    return new SelDouble(s);
  }

  double doubleVal() {
    return val;
  }

  @Override
  public Double getInternalVal() {
    return val;
  }

  @Override
  public SelTypes type() {
    return SelTypes.DOUBLE;
  }

  @Override
  public SelDouble assignOps(SelOp op, SelType rhs) {
    SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
    double another = ((SelDouble) rhs).val;
    switch (op) {
      case ASSIGN:
        this.val = another;
        return this;
      case ADD_ASSIGN:
        this.val += another;
        return this;
      case SUB_ASSIGN:
        this.val -= another;
        return this;
      case MUL_ASSIGN:
        this.val *= another;
        return this;
      case DIV_ASSIGN:
        this.val /= another;
        return this;
      case MOD_ASSIGN:
        this.val %= another;
        return this;
      default:
        throw new UnsupportedOperationException(
            "float/Float/double/Doubles DO NOT support assignment operation " + op);
    }
  }

  @Override
  public SelType binaryOps(SelOp op, SelType rhs) {
    if (rhs.type() == SelTypes.NULL) {
      if (op == SelOp.EQUAL || op == SelOp.NOT_EQUAL) {
        return rhs.binaryOps(op, this);
      } else {
        throw new UnsupportedOperationException(
            this.type() + " DO NOT support " + op + " for rhs with NULL value");
      }
    }

    if (rhs.type() == SelTypes.STRING) {
      return SelString.of(String.valueOf(this.val)).binaryOps(op, rhs);
    }

    double another = ((Number) rhs.getInternalVal()).doubleValue();
    switch (op) {
      case EQUAL:
        return SelBoolean.of(this.val == another);
      case NOT_EQUAL:
        return SelBoolean.of(this.val != another);
      case LT:
        return SelBoolean.of(this.val < another);
      case GT:
        return SelBoolean.of(this.val > another);
      case LTE:
        return SelBoolean.of(this.val <= another);
      case GTE:
        return SelBoolean.of(this.val >= another);
      case ADD:
        return new SelDouble(this.val + another);
      case SUB:
        return new SelDouble(this.val - another);
      case MUL:
        return new SelDouble(this.val * another);
      case DIV:
        return new SelDouble(this.val / another);
      case MOD:
        return new SelDouble(this.val % another);
      case PLUS:
        return new SelDouble(this.val);
      case MINUS:
        return new SelDouble(-this.val);
      default:
        throw new UnsupportedOperationException(
            "float/Float/double/Doubles DO NOT support expression operation " + op);
    }
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0 && "toString".equals(methodName)) {
      return SelString.of(Double.toString(val));
    } else if (args.length == 1 && "toString".equals(methodName)) {
      return SelString.of(((SelDouble) args[0]).toString());
    } else if (args.length == 1 && "valueOf".equals(methodName)) {
      switch (args[0].type()) {
        case STRING:
          return SelDouble.of(((SelString) args[0]).getInternalVal());
        case DOUBLE:
          return SelDouble.of(((SelDouble) args[0]).doubleVal());
      }
    } else if (args.length == 1 && "parseDouble".equals(methodName)) {
      return SelDouble.of(Double.parseDouble(((SelString) args[0]).getInternalVal()));
    } else if (args.length == 0 && "intValue".equals(methodName)) {
      return SelLong.of((long) this.val);
    } else if (args.length == 0 && "longValue".equals(methodName)) {
      return SelLong.of((long) this.val);
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
