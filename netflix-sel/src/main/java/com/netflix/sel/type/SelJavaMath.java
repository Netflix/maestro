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

/** Wrapper class to support java.lang.Math. */
public final class SelJavaMath implements SelType {

  static final SelJavaMath INSTANCE = new SelJavaMath();

  private SelJavaMath() {}

  @Override
  public SelTypes type() {
    return SelTypes.MATH;
  }

  @Override
  public String toString() {
    return "Java Math Class";
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if ("min".equals(methodName) && args.length == 2) {
      return SelLong.of(Math.min(((SelLong) args[0]).longVal(), ((SelLong) args[1]).longVal()));
    } else if ("max".equals(methodName) && args.length == 2) {
      return SelLong.of(Math.max(((SelLong) args[0]).longVal(), ((SelLong) args[1]).longVal()));
    } else if ("random".equals(methodName) && args.length == 0) {
      return SelDouble.of(Math.random());
    } else if ("pow".equals(methodName) && args.length == 2) {
      return SelDouble.of(
          Math.pow(
              ((Number) args[0].getInternalVal()).doubleValue(),
              ((Number) args[1].getInternalVal()).doubleValue()));
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }
}
