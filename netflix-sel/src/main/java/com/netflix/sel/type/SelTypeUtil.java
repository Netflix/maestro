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

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Internal util class */
public final class SelTypeUtil {

  public static void checkTypeMatch(SelTypes lhs, SelTypes rhs) {
    if (lhs != rhs) {
      throw new IllegalArgumentException(
          "Type mismatch, lhs type: " + lhs + ", rhs object type: " + rhs);
    }
  }

  static SelType callJavaMethod(Object javaObj, SelType[] args, MethodHandle m, String methodName) {
    try {
      if (args.length == 0) {
        return callJavaMethod0(javaObj, m);
      } else if (args.length == 1) {
        return callJavaMethod1(javaObj, args[0], m);
      } else if (args.length == 2) {
        return callJavaMethod2(javaObj, args[0], args[1], m);
      }
    } catch (IllegalStateException e) {
      throw e;
    } catch (Throwable t) {
      throw new IllegalArgumentException("Failed calling method " + methodName, t);
    }
    throw new UnsupportedOperationException(
        "DO NOT support calling method: " + methodName + " with args: " + Arrays.toString(args));
  }

  private static SelType callJavaMethod0(Object obj, MethodHandle m) throws Throwable {
    if (obj == null) {
      return box(m.invoke());
    } else {
      return box(m.invoke(obj));
    }
  }

  private static SelType callJavaMethod1(Object obj, SelType arg1, MethodHandle m)
      throws Throwable {
    Object obj1 = arg1.unbox();
    if (obj == null) {
      return box(m.invoke(obj1));
    } else {
      return box(m.invoke(obj, obj1));
    }
  }

  private static SelType callJavaMethod2(Object obj, SelType arg1, SelType arg2, MethodHandle m)
      throws Throwable {
    Object obj1 = arg1.unbox();
    Object obj2 = arg2.unbox();
    if (obj == null) {
      return box(m.invoke(obj1, obj2));
    } else {
      return box(m.invoke(obj, obj1, obj2));
    }
  }

  @SuppressWarnings("unchecked")
  public static SelType box(Object o) {
    if (o == null) { // returned null from a method, representing void or object
      return SelType.NULL;
    }
    SelTypes type = fromClazzToSelType(o.getClass());
    switch (type) {
      case STRING:
        return SelString.of((String) o);
      case LONG:
        return SelLong.of(((Number) o).longValue());
      case DOUBLE:
        return SelDouble.of(((Number) o).doubleValue());
      case BOOLEAN:
        return SelBoolean.of((Boolean) o);
      case STRING_ARRAY:
      case LONG_ARRAY:
      case DOUBLE_ARRAY:
      case BOOLEAN_ARRAY:
        return SelArray.of(o, type);
      case MAP:
        return SelMap.of((Map<String, Object>) o);
      case DATETIME:
        return SelJodaDateTime.of((DateTime) o);
      case DATETIME_PROPERTY:
        return SelJodaDateTimeProperty.of((DateTime.Property) o);
    }
    throw new UnsupportedOperationException(
        "Not support to box an object " + o + " for type " + type.name());
  }

  public static SelTypes fromStringToSelType(String clazz) {
    if (JAVA_CLAZZ_NAME_TO_SEL_TYPES.containsKey(clazz)) {
      return JAVA_CLAZZ_NAME_TO_SEL_TYPES.get(clazz);
    }
    throw new UnsupportedOperationException(
        "Please check the manual for supported "
            + "classes, which does not include class:  "
            + clazz);
  }

  private static final Map<String, SelTypes> JAVA_CLAZZ_NAME_TO_SEL_TYPES;

  static {
    Map<String, SelTypes> map = new HashMap<>();
    map.put("int", SelTypes.LONG);
    map.put("long", SelTypes.LONG);
    map.put("Integer", SelTypes.LONG);
    map.put("Long", SelTypes.LONG);
    map.put("float", SelTypes.DOUBLE);
    map.put("double", SelTypes.DOUBLE);
    map.put("Float", SelTypes.DOUBLE);
    map.put("Double", SelTypes.DOUBLE);
    map.put("boolean", SelTypes.BOOLEAN);
    map.put("Boolean", SelTypes.BOOLEAN);
    map.put("String", SelTypes.STRING);

    map.put("Map", SelTypes.MAP); // it is always String -> any MelType
    map.put("HashMap", SelTypes.MAP);
    map.put("LinkedHashMap", SelTypes.MAP);

    map.put("DateTime", SelTypes.DATETIME);
    map.put("DateTimeZone", SelTypes.DATETIME_ZONE);
    map.put("DateTimeFormatter", SelTypes.DATETIME_FORMATTER);
    map.put("DateTimeFormat", SelTypes.DATETIME_FORMATTER);
    map.put("Property", SelTypes.DATETIME_PROPERTY);
    map.put("Days", SelTypes.DATETIME_DAYS);

    map.put("UUID", SelTypes.UUID);
    JAVA_CLAZZ_NAME_TO_SEL_TYPES = Collections.unmodifiableMap(map);
  }

  private static final Map<Class<?>, SelTypes> JAVA_CLAZZ_TO_SEL_TYPES;

  static {
    Map<Class<?>, SelTypes> map = new HashMap<>();
    map.put(String.class, SelTypes.STRING);
    map.put(Integer.class, SelTypes.LONG);
    map.put(Long.class, SelTypes.LONG);
    map.put(Float.class, SelTypes.DOUBLE);
    map.put(Double.class, SelTypes.DOUBLE);
    map.put(Boolean.class, SelTypes.BOOLEAN);

    map.put(String[].class, SelTypes.STRING_ARRAY);
    map.put(Long[].class, SelTypes.LONG_ARRAY);
    map.put(long[].class, SelTypes.LONG_ARRAY);
    map.put(Integer[].class, SelTypes.LONG_ARRAY);
    map.put(int[].class, SelTypes.LONG_ARRAY);
    map.put(Double[].class, SelTypes.DOUBLE_ARRAY);
    map.put(double[].class, SelTypes.DOUBLE_ARRAY);
    map.put(Float[].class, SelTypes.DOUBLE_ARRAY);
    map.put(float[].class, SelTypes.DOUBLE_ARRAY);
    map.put(Boolean[].class, SelTypes.BOOLEAN_ARRAY);
    map.put(boolean[].class, SelTypes.BOOLEAN_ARRAY);

    map.put(Map.class, SelTypes.MAP);
    map.put(HashMap.class, SelTypes.MAP);
    map.put(LinkedHashMap.class, SelTypes.MAP);

    map.put(DateTime.class, SelTypes.DATETIME);
    map.put(DateTimeZone.class, SelTypes.DATETIME_ZONE);
    map.put(DateTimeFormatter.class, SelTypes.DATETIME_FORMATTER);
    map.put(DateTimeFormat.class, SelTypes.DATETIME_FORMATTER);
    map.put(DateTime.Property.class, SelTypes.DATETIME_PROPERTY);
    map.put(Days.class, SelTypes.DATETIME_DAYS);

    JAVA_CLAZZ_TO_SEL_TYPES = Collections.unmodifiableMap(map);
  }

  private static SelTypes fromClazzToSelType(Class<?> clazz) {
    if (JAVA_CLAZZ_TO_SEL_TYPES.containsKey(clazz)) {
      return JAVA_CLAZZ_TO_SEL_TYPES.get(clazz);
    }
    throw new UnsupportedOperationException("Return type " + clazz + " is not supported.");
  }

  public static final Map<String, SelType> STATIC_OBJECTS;

  static {
    Map<String, SelType> map = new HashMap<>();
    for (Map.Entry<String, SelTypes> entry : JAVA_CLAZZ_NAME_TO_SEL_TYPES.entrySet()) {
      map.put(entry.getKey(), entry.getValue().newSelTypeObj());
    }

    // manually put preset mapping
    map.put("DateTimeConstants", SelMiscFunc.INSTANCE);
    map.put("System", SelMiscFunc.INSTANCE);
    map.put("Arrays", SelMiscFunc.INSTANCE);

    map.put("Math", SelJavaMath.INSTANCE);
    map.put("Util", SelUtilFunc.INSTANCE);

    STATIC_OBJECTS = Collections.unmodifiableMap(map);
  }

  /**
   * Preprocess string literal by removing the first and last quotations. Then, unescape it without
   * supporting Octal or Unicode.
   */
  public static String preprocess(String literal) {
    if (literal == null) {
      return null;
    }

    StringBuilder sb = new StringBuilder(literal.length() - 2);

    for (int i = 1; i < literal.length() - 1; i++) {
      char ch = literal.charAt(i);
      if (ch == '\\') {
        if (i >= literal.length() - 2) {
          throw new IllegalArgumentException("Invalid escaped literal string: " + literal);
        }
        char next = literal.charAt(++i);
        switch (next) {
          case 'b':
            ch = '\b';
            break;
          case 'n':
            ch = '\n';
            break;
          case 't':
            ch = '\t';
            break;
          case 'f':
            ch = '\f';
            break;
          case 'r':
            ch = '\r';
            break;
          case '\\':
            ch = '\\';
            break;
          case '\"':
            ch = '\"';
            break;
          case '\'':
            ch = '\'';
            break;
          default:
            throw new IllegalArgumentException("Invalid escaped literal string: " + literal);
        }
      }
      sb.append(ch);
    }
    return sb.toString();
  }
}
