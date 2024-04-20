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

/** Enum for all supported classes */
public enum SelTypes implements SelType {
  // data type
  STRING,
  LONG,
  DOUBLE,
  BOOLEAN,

  STRING_ARRAY,
  LONG_ARRAY,
  DOUBLE_ARRAY,
  BOOLEAN_ARRAY,

  MAP,

  // internal type
  SEL_TYPES,
  NULL,
  VOID,
  ERROR,
  PRESET_PARAMS, // preset params, immutable
  PRESET_UTIL_FUNCTION,
  PRESET_MISC_FUNC_FIELDS,

  // Joda DateTime
  DATETIME,
  DATETIME_ZONE,
  DATETIME_FORMATTER,
  DATETIME_PROPERTY,
  DATETIME_DAYS,

  // Java Util
  UUID,
  MATH;

  @Override
  public SelTypes type() {
    return SEL_TYPES;
  }

  public static final String CONSTRUCTOR = "constructor";

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (CONSTRUCTOR.equals(methodName)) {
      switch (this) {
        case STRING:
          return SelString.create(args);
        case LONG:
          return SelLong.create(args);
        case DOUBLE:
          return SelDouble.create(args);
        case BOOLEAN:
          return SelBoolean.create(args);
        case MAP:
          return SelMap.create(args);
        case DATETIME:
          return SelJodaDateTime.create(args);
      }
    }
    throw new UnsupportedOperationException(this.name() + " DO NOT support calling " + methodName);
  }

  public SelArray newSelTypeObjArray(int len) {
    switch (this) {
      case STRING:
        return new SelArray(len, SelTypes.STRING_ARRAY);
      case LONG:
        return new SelArray(len, SelTypes.LONG_ARRAY);
      case DOUBLE:
        return new SelArray(len, SelTypes.DOUBLE_ARRAY);
      case BOOLEAN:
        return new SelArray(len, SelTypes.BOOLEAN_ARRAY);
      default:
        throw new UnsupportedOperationException(
            "NOT support creating an array with the element type " + this.name());
    }
  }

  public SelType newSelTypeObj() {
    switch (this) {
      case STRING:
        return SelString.of(null);
      case LONG:
        return SelLong.of(0L);
      case DOUBLE:
        return SelDouble.of(0.0);
      case BOOLEAN:
        return SelBoolean.of(false);
      case STRING_ARRAY:
        return new SelArray(0, STRING_ARRAY);
      case LONG_ARRAY:
        return new SelArray(0, LONG_ARRAY);
      case DOUBLE_ARRAY:
        return new SelArray(0, DOUBLE_ARRAY);
      case BOOLEAN_ARRAY:
        return new SelArray(0, BOOLEAN_ARRAY);
      case MAP:
        return SelMap.of(null);

      case DATETIME:
        return SelJodaDateTime.of(null);
      case DATETIME_ZONE:
        return SelJodaDateTimeZone.of(null);
      case DATETIME_DAYS:
        return SelJodaDateTimeDays.of(null);
      case DATETIME_PROPERTY:
        return SelJodaDateTimeProperty.of(null);
      case DATETIME_FORMATTER:
        return SelJodaDateTimeFormatter.of(null);

      case UUID:
        return SelJavaUUID.of(null);
      default:
        throw new UnsupportedOperationException(
            "Not support creating an empty object for type " + this.name());
    }
  }
}
