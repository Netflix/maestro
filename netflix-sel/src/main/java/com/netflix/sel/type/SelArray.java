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
import java.lang.reflect.Array;
import java.util.Arrays;

/** Wrapper class to support 4 types of arrays. */
public final class SelArray extends AbstractSelType {
  private SelType[] val;
  private final SelTypes type;

  private SelArray(SelType[] vals, SelTypes type) {
    this.val = vals;
    this.type = type;
  }

  SelArray(int len, SelTypes type) {
    switch (type) {
      case STRING_ARRAY:
        val = new SelString[len];
        for (int i = 0; i < val.length; ++i) {
          val[i] = SelString.of(null);
        }
        break;
      case LONG_ARRAY:
        val = new SelLong[len];
        for (int i = 0; i < val.length; ++i) {
          val[i] = SelLong.of(0L);
        }
        break;
      case DOUBLE_ARRAY:
        val = new SelDouble[len];
        for (int i = 0; i < val.length; ++i) {
          val[i] = SelDouble.of(0.0);
        }
        break;
      case BOOLEAN_ARRAY:
        val = new SelBoolean[len];
        for (int i = 0; i < val.length; ++i) {
          val[i] = SelBoolean.of(false);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Not support to create an empty array for type " + type);
    }
    this.type = type;
  }

  static SelArray of(Object o, SelTypes type) {
    switch (type) {
      case STRING_ARRAY:
        SelString[] arrays = new SelString[Array.getLength(o)];
        for (int i = 0; i < arrays.length; ++i) {
          arrays[i] = SelString.of(((String) Array.get(o, i)));
        }
        return new SelArray(arrays, SelTypes.STRING_ARRAY);
      case LONG_ARRAY:
        SelLong[] arrayl = new SelLong[Array.getLength(o)];
        for (int i = 0; i < arrayl.length; ++i) {
          arrayl[i] = SelLong.of(((Number) Array.get(o, i)).longValue());
        }
        return new SelArray(arrayl, SelTypes.LONG_ARRAY);
      case DOUBLE_ARRAY:
        SelDouble[] arrayd = new SelDouble[Array.getLength(o)];
        for (int i = 0; i < arrayd.length; ++i) {
          arrayd[i] = SelDouble.of(((Number) Array.get(o, i)).doubleValue());
        }
        return new SelArray(arrayd, SelTypes.DOUBLE_ARRAY);
      case BOOLEAN_ARRAY:
        SelBoolean[] arrayb = new SelBoolean[Array.getLength(o)];
        for (int i = 0; i < arrayb.length; ++i) {
          arrayb[i] = SelBoolean.of((Boolean) Array.get(o, i));
        }
        return new SelArray(arrayb, SelTypes.BOOLEAN_ARRAY);
    }
    throw new UnsupportedOperationException(
        "Not support to copy " + o + " to an array with type " + type);
  }

  public void set(int idx, SelType obj) {
    val[idx].assignOps(SelOp.ASSIGN, obj);
  }

  public SelType get(int idx) {
    return val[idx];
  }

  @Override
  public SelType[] getInternalVal() {
    return val;
  }

  @Override
  public SelTypes type() {
    return this.type;
  }

  @Override
  public SelArray assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelArray) rhs).val; // direct assignment
      return this;
    }
    throw new UnsupportedOperationException(
        this.type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public Object unbox() {
    switch (type) {
      case STRING_ARRAY:
        String[] arrays = new String[val.length];
        for (int i = 0; i < arrays.length; ++i) {
          arrays[i] = ((SelString) val[i]).getInternalVal();
        }
        return arrays;
      case LONG_ARRAY:
        long[] arrayl = new long[val.length];
        for (int i = 0; i < arrayl.length; ++i) {
          arrayl[i] = ((SelLong) val[i]).longVal();
        }
        return arrayl;
      case DOUBLE_ARRAY:
        double[] arrayd = new double[val.length];
        for (int i = 0; i < arrayd.length; ++i) {
          arrayd[i] = ((SelDouble) val[i]).doubleVal();
        }
        return arrayd;
      case BOOLEAN_ARRAY:
        boolean[] arrayb = new boolean[val.length];
        for (int i = 0; i < arrayb.length; ++i) {
          arrayb[i] = ((SelBoolean) val[i]).booleanVal();
        }
        return arrayb;
    }
    throw new UnsupportedOperationException("Not support to unbox an array with type " + type);
  }

  @Override
  public SelLong field(SelString field) {
    String fieldName = field.getInternalVal();
    if ("length".equals(fieldName)) {
      return SelLong.of(val.length);
    }
    throw new UnsupportedOperationException(type() + " DO NOT support accessing field: " + field);
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if ("contains".equals(methodName) && args.length == 1) {
      return contains(args[0]);
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  private SelBoolean contains(SelType element) {
    for (SelType item : val) {
      SelBoolean isEqual = (SelBoolean) item.binaryOps(SelOp.EQUAL, element);
      if (isEqual.booleanVal()) {
        return SelBoolean.of(true);
      }
    }
    return SelBoolean.of(false);
  }

  @Override
  public String toString() {
    return Arrays.toString(val);
  }
}
