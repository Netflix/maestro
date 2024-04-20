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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Wrapper class to support String data type. */
public final class SelString extends AbstractSelType {

  private String val;

  public static SelString of(String s) {
    return new SelString(s);
  }

  private SelString(String val) {
    super(val == null ? 0 : val.length() * 2L);
    this.val = val;
  }

  static SelString create(SelType arg) {
    switch (arg.type()) {
      case LONG:
        return new SelString(String.valueOf(((SelLong) arg).longVal()));
      case STRING:
        return new SelString(((SelString) arg).val);
    }
    throw new IllegalArgumentException(
        "Invalid input argument (" + arg + ") for String constructor");
  }

  static SelString create(SelType[] args) {
    if (args.length == 1) {
      return create(args[0]);
    } else if (args.length == 0) { // this is what Java supports
      return new SelString("");
    }
    throw new IllegalArgumentException(
        "Invalid input arguments (" + Arrays.toString(args) + ") for String constructor");
  }

  private static final Map<String, MethodHandle> SUPPORTED_METHODS;

  static {
    Map<String, MethodHandle> map = new HashMap<>();
    try {
      map.put(
          "split1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "split", MethodType.methodType(String[].class, String.class)));
      map.put(
          "split2",
          MethodHandles.lookup()
              .findVirtual(
                  String.class,
                  "split",
                  MethodType.methodType(String[].class, String.class, int.class))
              .asType(
                  MethodType.methodType(
                      String[].class, String.class, String.class, Integer.class)));
      map.put(
          "toUpperCase0",
          MethodHandles.lookup()
              .findVirtual(String.class, "toUpperCase", MethodType.methodType(String.class)));
      map.put(
          "toLowerCase0",
          MethodHandles.lookup()
              .findVirtual(String.class, "toLowerCase", MethodType.methodType(String.class)));
      map.put(
          "replaceAll2",
          MethodHandles.lookup()
              .findVirtual(
                  String.class,
                  "replaceAll",
                  MethodType.methodType(String.class, String.class, String.class)));
      map.put(
          "replace2",
          MethodHandles.lookup()
              .findVirtual(
                  String.class,
                  "replace",
                  MethodType.methodType(String.class, CharSequence.class, CharSequence.class))
              .asType(
                  MethodType.methodType(String.class, String.class, String.class, String.class)));
      map.put(
          "valueOf1",
          MethodHandles.lookup()
              .findStatic(
                  String.class, "valueOf", MethodType.methodType(String.class, Object.class)));

      map.put(
          "startsWith1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "startsWith", MethodType.methodType(boolean.class, String.class))
              .asType(MethodType.methodType(Boolean.class, String.class, String.class)));
      map.put(
          "endsWith1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "endsWith", MethodType.methodType(boolean.class, String.class))
              .asType(MethodType.methodType(Boolean.class, String.class, String.class)));
      map.put(
          "contains1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class,
                  "contains",
                  MethodType.methodType(boolean.class, CharSequence.class))
              .asType(MethodType.methodType(Boolean.class, String.class, String.class)));

      map.put(
          "substring1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "substring", MethodType.methodType(String.class, int.class))
              .asType(MethodType.methodType(String.class, String.class, Integer.class)));
      map.put(
          "substring2",
          MethodHandles.lookup()
              .findVirtual(
                  String.class,
                  "substring",
                  MethodType.methodType(String.class, int.class, int.class))
              .asType(
                  MethodType.methodType(String.class, String.class, Integer.class, Integer.class)));

      map.put(
          "lastIndexOf1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "lastIndexOf", MethodType.methodType(int.class, String.class))
              .asType(MethodType.methodType(Integer.class, String.class, String.class)));
      map.put(
          "length0",
          MethodHandles.lookup()
              .findVirtual(String.class, "length", MethodType.methodType(int.class))
              .asType(MethodType.methodType(Integer.class, String.class)));
      map.put(
          "isEmpty0",
          MethodHandles.lookup()
              .findVirtual(String.class, "isEmpty", MethodType.methodType(boolean.class))
              .asType(MethodType.methodType(Boolean.class, String.class)));
      map.put(
          "equals1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "equals", MethodType.methodType(boolean.class, Object.class))
              .asType(MethodType.methodType(Boolean.class, String.class, Object.class)));
      map.put(
          "compareTo1",
          MethodHandles.lookup()
              .findVirtual(
                  String.class, "compareTo", MethodType.methodType(int.class, String.class))
              .asType(MethodType.methodType(Integer.class, String.class, String.class)));
      map.put(
          "trim0",
          MethodHandles.lookup()
              .findVirtual(String.class, "trim", MethodType.methodType(String.class)));
    } catch (Exception ex) {
      throw new RuntimeException("Initialization failure in STRING static block.", ex);
    }
    SUPPORTED_METHODS = Collections.unmodifiableMap(map);
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if ("format".equals(methodName)) { // static varargs format method
      return varArgsFormat(args);
    }
    if ("join".equals(methodName)) { // static varargs join methods
      return varArgsJoin(args);
    }
    if ("escape".equals(methodName)) {
      return escapeJava(args);
    }
    methodName += args.length;
    if (SUPPORTED_METHODS.containsKey(methodName)) {
      return SelTypeUtil.callJavaMethod(val, args, SUPPORTED_METHODS.get(methodName), methodName);
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  private static SelString varArgsFormat(SelType[] args) {
    if (args.length > 0) {
      String arg0 = ((SelString) args[0]).val;
      if (args.length == 2 && args[1] instanceof SelArray) {
        return SelString.of(String.format(arg0, args[1].unbox()));
      } else {
        Object[] varargs = new Object[args.length - 1];
        for (int i = 0; i < varargs.length; ++i) {
          varargs[i] = args[i + 1].unbox();
        }
        return SelString.of(String.format(arg0, varargs));
      }
    }
    throw new IllegalArgumentException("Invalid empty input args for String.format method");
  }

  private static SelString varArgsJoin(SelType[] args) {
    if (args.length > 0) {
      String arg0 = ((SelString) args[0]).val;
      if (args.length == 2 && args[1] instanceof SelArray) {
        return SelString.of(String.join(arg0, (String[]) args[1].unbox()));
      } else {
        CharSequence[] varargs = new CharSequence[args.length - 1];
        for (int i = 0; i < varargs.length; ++i) {
          varargs[i] = ((SelString) args[i + 1]).val;
        }
        return SelString.of(String.join(arg0, varargs));
      }
    }
    throw new IllegalArgumentException("Invalid empty input args for String.format method");
  }

  private SelString escapeJava(SelType[] args) {
    String s = null;
    if (args.length == 0) {
      s = this.val;
    } else if (args.length == 1 && args[0].type() == SelTypes.STRING) {
      s = args[0].toString();
    } else {
      throw new IllegalArgumentException("Invalid input args for String.escape method");
    }
    // based on https://docs.oracle.com/javase/tutorial/java/data/characters.html
    s =
        s.replace("\\", "\\\\")
            .replace("\t", "\\t")
            .replace("\b", "\\b")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\f", "\\f")
            //        .replace("\'", "\\'")  <-- unnecessary and extra escaping will cause issues
            .replace("\"", "\\\"");
    return SelString.of(s);
  }

  @Override
  public String getInternalVal() {
    return val;
  }

  @Override
  public SelTypes type() {
    return SelTypes.STRING;
  }

  @Override
  public SelString assignOps(SelOp op, SelType rhs) {
    switch (op) {
      case ASSIGN:
        SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
        this.val = ((SelString) rhs).val; // direct assignment
        return this;
      case ADD_ASSIGN:
        this.val += rhs.getInternalVal();
        return this;
      default:
        throw new UnsupportedOperationException(
            type() + " DO NOT support assignment operation " + op);
    }
  }

  @Override
  public SelType binaryOps(SelOp op, SelType rhs) {
    if (rhs.type() != SelTypes.NULL && (op == SelOp.EQUAL || op == SelOp.NOT_EQUAL)) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
    }

    switch (op) {
      case ADD:
        return new SelString(this.val + rhs.getInternalVal());
      case EQUAL:
        return SelBoolean.of(Objects.equals(this.val, rhs.getInternalVal()));
      case NOT_EQUAL:
        return SelBoolean.of(!Objects.equals(this.val, rhs.getInternalVal()));
      default:
        throw new UnsupportedOperationException(
            type() + " DO NOT support expression operation " + op);
    }
  }

  @Override
  public String toString() {
    return String.valueOf(val);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof SelString) {
      return Objects.equals(val, ((SelString) obj).val);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(val);
  }
}
