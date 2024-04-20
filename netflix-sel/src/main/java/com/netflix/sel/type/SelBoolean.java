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

/** Wrapper class to support boolean data type. */
public final class SelBoolean extends AbstractSelType {
  private boolean val;

  private SelBoolean(boolean b) {
    this.val = b;
  }

  static SelBoolean create(SelType[] args) {
    if (args.length == 1) {
      switch (args[0].type()) {
        case BOOLEAN:
          return new SelBoolean(((SelBoolean) args[0]).val);
        case STRING:
          return SelBoolean.of(Boolean.parseBoolean(((SelString) args[0]).getInternalVal()));
      }
    }
    throw new IllegalArgumentException(
        "Invalid input arguments (" + Arrays.toString(args) + ") for Boolean constructor");
  }

  public boolean booleanVal() {
    return val;
  }

  public static SelBoolean of(boolean b) {
    return new SelBoolean(b);
  }

  @Override
  public Boolean getInternalVal() {
    return val;
  }

  @Override
  public SelTypes type() {
    return SelTypes.BOOLEAN;
  }

  @Override
  public SelBoolean assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelBoolean) rhs).val; // direct assignment
      return this;
    }
    throw new UnsupportedOperationException(
        "boolean/Boolean DO NOT support assignment operation " + op);
  }

  @Override
  public SelBoolean binaryOps(SelOp op, SelType rhs) {
    if (rhs.type() == SelTypes.NULL && (op == SelOp.EQUAL || op == SelOp.NOT_EQUAL)) {
      return (SelBoolean) rhs.binaryOps(op, this);
    }

    SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
    boolean another = ((SelBoolean) rhs).booleanVal();
    switch (op) {
      case AND:
        return SelBoolean.of(val && another);
      case OR:
        return SelBoolean.of(val || another);
      case EQUAL:
        return SelBoolean.of(val == another);
      case NOT_EQUAL:
        return SelBoolean.of(val != another);
      case NOT:
        return SelBoolean.of(!val);
      default:
        throw new UnsupportedOperationException(
            this.type() + " DO NOT support expression operation " + op);
    }
  }

  @Override
  public String toString() {
    return String.valueOf(val);
  }
}
