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

import java.util.Arrays;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeUtils;

/** Util class to include static methods */
public final class SelMiscFunc implements SelType {

  static final SelMiscFunc INSTANCE = new SelMiscFunc();

  private SelMiscFunc() {}

  @Override
  public SelTypes type() {
    return SelTypes.PRESET_MISC_FUNC_FIELDS;
  }

  @Override
  public String toString() {
    return "SEL Miscellaneous Function Class";
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0 && "currentTimeMillis".equals(methodName)) {
      return SelLong.of(DateTimeUtils.currentTimeMillis());
    }
    // no-op to support Arrays.asList
    if (args.length == 1 && "asList".equals(methodName)) {
      return args[0];
    }

    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  @Override
  public SelLong field(SelString field) {
    String fieldName = field.getInternalVal();
    if ("SUNDAY".equals(fieldName)) {
      return SelLong.of(DateTimeConstants.SUNDAY);
    }
    throw new UnsupportedOperationException(type() + " DO NOT support accessing field: " + field);
  }
}
