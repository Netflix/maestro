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
import java.util.HashMap;
import java.util.Map;

/** Wrapper class to support Map<String, Object> data type */
public final class SelMap extends AbstractSelType {

  private Map<SelString, SelType> val;

  private SelMap(Map<SelString, SelType> v) {
    this.val = v;
  }

  static SelMap create(SelType[] args) {
    if (args.length == 0) {
      return new SelMap(new HashMap<>());
    }
    throw new IllegalArgumentException(
        "Invalid input arguments (" + Arrays.toString(args) + ") for Map constructor");
  }

  @SuppressWarnings("unchecked")
  static SelMap of(Map<String, Object> input) {
    if (input == null) {
      return new SelMap(null);
    }
    SelMap map = new SelMap(new HashMap<>());
    for (Map.Entry<String, Object> entry : input.entrySet()) {
      if (entry.getValue() instanceof String) {
        map.val.put(SelString.of(entry.getKey()), SelString.of((String) entry.getValue()));
      } else if (entry.getValue() instanceof Long || entry.getValue() instanceof Integer) {
        map.val.put(
            SelString.of(entry.getKey()), SelLong.of(((Number) entry.getValue()).longValue()));
      } else if (entry.getValue() instanceof Double || entry.getValue() instanceof Float) {
        map.val.put(
            SelString.of(entry.getKey()), SelDouble.of(((Number) entry.getValue()).doubleValue()));
      } else if (entry.getValue() instanceof Boolean) {
        map.val.put(SelString.of(entry.getKey()), SelBoolean.of((Boolean) entry.getValue()));
      } else if (entry.getValue() instanceof Map) {
        map.val.put(
            SelString.of(entry.getKey()), SelMap.of((Map<String, Object>) entry.getValue()));
      } else if (entry.getValue() instanceof String[]) {
        map.val.put(
            SelString.of(entry.getKey()), SelArray.of(entry.getValue(), SelTypes.STRING_ARRAY));
      } else if (entry.getValue() instanceof long[]) {
        map.val.put(
            SelString.of(entry.getKey()), SelArray.of(entry.getValue(), SelTypes.LONG_ARRAY));
      } else if (entry.getValue() instanceof double[]) {
        map.val.put(
            SelString.of(entry.getKey()), SelArray.of(entry.getValue(), SelTypes.DOUBLE_ARRAY));
      } else if (entry.getValue() instanceof boolean[]) {
        map.val.put(
            SelString.of(entry.getKey()), SelArray.of(entry.getValue(), SelTypes.BOOLEAN_ARRAY));
      } else {
        throw new IllegalArgumentException(
            "Invalid map entry type ("
                + (entry.getValue() == null ? "null" : entry.getValue().getClass().getName())
                + ") for Map constructor");
      }
    }
    return map;
  }

  @Override
  public Map<SelString, SelType> getInternalVal() {
    return val;
  }

  @Override
  public SelMap assignOps(SelOp op, SelType rhs) {
    if (op == SelOp.ASSIGN) {
      SelTypeUtil.checkTypeMatch(this.type(), rhs.type());
      this.val = ((SelMap) rhs).val; // direct assignment
      return this;
    }
    throw new UnsupportedOperationException(type() + " DO NOT support assignment operation " + op);
  }

  @Override
  public SelTypes type() {
    return SelTypes.MAP;
  }

  @Override
  public Map<String, Object> unbox() {
    if (val == null) {
      return null;
    }
    Map<String, Object> ret = new HashMap<>();
    for (Map.Entry<SelString, SelType> entry : val.entrySet()) {
      switch (entry.getValue().type()) {
        case STRING:
        case LONG:
        case DOUBLE:
        case BOOLEAN:
          ret.put(entry.getKey().getInternalVal(), entry.getValue().getInternalVal());
          break;
        case STRING_ARRAY:
        case LONG_ARRAY:
        case DOUBLE_ARRAY:
        case BOOLEAN_ARRAY:
        case MAP:
          ret.put(entry.getKey().getInternalVal(), entry.getValue().unbox());
          break;
        default:
          throw new UnsupportedOperationException(
              "Invalid type, not support having map entry value " + entry);
      }
    }
    return ret;
  }

  @Override
  public SelType call(String methodName, SelType[] args) {
    if (args.length == 0 && "size".equals(methodName)) {
      return SelLong.of(val == null ? 0 : val.size());
    } else if (args.length == 1 && "get".equals(methodName)) {
      if (!val.containsKey((SelString) args[0])) {
        return NULL;
      }
      return val.get((SelString) args[0]);
    } else if (args.length == 1 && "containsKey".equals(methodName)) {
      return SelBoolean.of(val != null && val.containsKey((SelString) args[0]));
    } else if (args.length == 2 && "put".equals(methodName)) {
      SelType value = args[1] == null ? NULL : args[1];
      SelType res = val.put((SelString) args[0], value);
      if (res == null) {
        return NULL;
      }
      return res;
    } else if (args.length == 2 && "getOrDefault".equals(methodName)) {
      if (!val.containsKey((SelString) args[0])) {
        return args[1];
      }
      return val.get((SelString) args[0]);
    }
    throw new UnsupportedOperationException(
        type()
            + " DO NOT support calling method: "
            + methodName
            + " with args: "
            + Arrays.toString(args));
  }

  @Override
  public String toString() {
    return String.valueOf(val);
  }
}
