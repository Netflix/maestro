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
import java.util.Random;
import java.util.UUID;

/**
 * Wrapper class to support java.util.UUID.
 *
 * <p>For randomUUID(), the wrapper provides two random long numbers to generate it instead of using
 * UUID.randomUUID() to keep it simple and safe. Note that UUID.randomUUID() requires a few security
 * permissions e.g. FilePermission("/dev/random", "read").
 */
public final class SelJavaUUID extends AbstractSelType {
  private static final Random rnd = new Random();

  private final UUID val;

  private SelJavaUUID(UUID val) {
    this.val = val;
  }

  static SelJavaUUID of(UUID d) {
    return new SelJavaUUID(d);
  }

  @Override
  public SelTypes type() {
    return SelTypes.UUID;
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0) {
      if ("randomUUID".equals(methodName)) {
        return SelJavaUUID.of(new UUID(rnd.nextLong(), rnd.nextLong()));
      } else if ("toString".equals(methodName)) {
        return SelString.of(String.valueOf(val));
      }
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }
}
