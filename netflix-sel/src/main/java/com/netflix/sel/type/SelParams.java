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

import com.netflix.sel.ext.Extension;
import java.util.Arrays;
import java.util.Map;

/**
 * Wrapper class to hold a special params field in Maestro eval logic. Users can use it to access
 * input params instead of directly using param name
 */
public final class SelParams implements SelType {
  private final Map<String, Object> val;
  private final Extension extension;

  private SelParams(Map<String, Object> m, Extension ext) {
    this.val = m;
    this.extension = ext;
  }

  public static SelParams of(Map<String, Object> input, Extension ext) {
    return new SelParams(input, ext);
  }

  @Override
  public SelTypes type() {
    return SelTypes.PRESET_PARAMS;
  }

  @Override
  public String toString() {
    return "PRESET_PARAMS";
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 1 && "get".equals(methodName)) {
      return field((SelString) args[0]);
    } else if (args.length == 1 && "containsKey".equals(methodName)) {
      return SelBoolean.of(val.containsKey(((SelString) args[0]).getInternalVal()));
    } else if (extension != null) {
      return extension.call(methodName, args);
    }

    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  @Override
  public SelType field(SelString field) {
    String key = field.getInternalVal();
    if (!val.containsKey(key)) {
      return NULL;
    }
    return SelTypeUtil.box(val.get(key));
  }
}
