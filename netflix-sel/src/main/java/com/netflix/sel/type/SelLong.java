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

/** Wrapper class to support int/Integer/long/Long data type. */
public final class SelLong extends AbstractSelType {

  private long val;

  private SelLong(long val) {
    this.val = val;
  }

  static SelLong create(SelType arg) {
    switch (arg.type()) {
      case LONG:
        return new SelLong(((SelLong) arg).val);
      case STRING:
        return SelLong.of(((SelString) arg).getInternalVal());
    }
    throw new IllegalArgumentException("Invalid input argument (" + arg + ") for Long constructor");
  }

  static SelLong create(SelType[] args) {
    if (args.length == 1) {
      return create(args[0]);
    }
    throw new IllegalArgumentException(
        "Invalid input arguments (" + Arrays.toString(args) + ") for Long constructor");
  }

  public static SelLong of(String s) {
    return new SelLong(Long.parseLong(s));
  }

  static SelLong of(long s) {
    return new SelLong(s);
  }

  public int intVal() {
    return (int) val;
  }

  long longVal() {
    return val;
  }

  @Override
  public Long getInternalVal() {
    return val;
  }

  @Override
  public SelTypes type() {
    return SelTypes.LONG;
  }

  @Override
  public SelLong assignOps(SelOp op, SelType rhs) {
    SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
    long another = ((SelLong) rhs).val;
    switch (op) {
      case ASSIGN:
        this.val = another; // direct assignment
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
            "int/Integer/long/Long DO NOT support assignment operation " + op);
    }
  }

  @Override
  public SelType binaryOps(SelOp op, SelType rhs) {
    if (rhs.type() == SelTypes.NULL && (op == SelOp.EQUAL || op == SelOp.NOT_EQUAL)) {
      return rhs.binaryOps(op, this);
    }

    if (rhs.type() == SelTypes.DOUBLE) {
      SelDouble lhs = SelDouble.of(this.val);
      return lhs.binaryOps(op, rhs);
    }

    if (rhs.type() == SelTypes.STRING) {
      return SelString.of(String.valueOf(this.val)).binaryOps(op, rhs);
    }

    SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
    long another = ((SelLong) rhs).val;
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
        return new SelLong(this.val + another);
      case SUB:
        return new SelLong(this.val - another);
      case MUL:
        return new SelLong(this.val * another);
      case DIV:
        return new SelLong(this.val / another);
      case MOD:
        return new SelLong(this.val % another);
      case PLUS:
        return new SelLong(this.val);
      case MINUS:
        return new SelLong(-this.val);
      default:
        throw new UnsupportedOperationException(
            "int/Integer/long/Long DO NOT support expression operation " + op);
    }
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if ("toString".equals(methodName)) {
      if (args.length == 0) {
        return SelString.of(Long.toString(val));
      } else if (args.length == 1) {
        return SelString.of(Long.toString(((SelLong) args[0]).val));
      }
    } else if (args.length == 1 && "valueOf".equals(methodName)) {
      switch (args[0].type()) {
        case STRING:
          return SelLong.of(((SelString) args[0]).getInternalVal());
        case LONG:
          return SelLong.of(((SelLong) args[0]).longVal());
      }
    } else if (args.length == 1 && "parseLong".equals(methodName)) {
      return SelLong.of(Long.parseLong(((SelString) args[0]).getInternalVal()));
    } else if (args.length == 0 && "intValue".equals(methodName)) {
      return SelLong.of(this.val);
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  // unbox to int if possible (used as input args for method calls)
  @Override
  public Object unbox() {
    if (val > Integer.MAX_VALUE) {
      return longVal();
    }
    return intVal();
  }

  @Override
  public String toString() {
    return String.valueOf(val);
  }
}
