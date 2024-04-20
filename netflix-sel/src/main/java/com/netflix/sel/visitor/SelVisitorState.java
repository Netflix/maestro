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
package com.netflix.sel.visitor;

import static com.netflix.sel.type.SelTypeUtil.STATIC_OBJECTS;
import static com.netflix.sel.type.SelTypeUtil.box;

import com.netflix.sel.ext.Extension;
import com.netflix.sel.type.SelParams;
import com.netflix.sel.type.SelType;
import com.netflix.sel.type.SelTypes;
import com.netflix.sel.util.MemoryCounter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Reusable stateful object to hold states used by evaluation */
final class SelVisitorState {

  private final Map<String, SelType> symtab; // internal symbol table
  private final SelType[] stack;
  private int top = -1;
  long visited = 0;

  private Map<String, Object> inputTab; // input variables

  SelVisitorState(int stackLimit) {
    this.symtab = new HashMap<>();
    this.stack = new SelType[stackLimit];
  }

  void clear() {
    if (top != -1) {
      Arrays.fill(stack, 0, top + 1, null);
      top = -1;
    }
    this.symtab.clear();
    this.inputTab = null;
    this.visited = 0;
    MemoryCounter.reset();
  }

  // call it before evaluation.
  void resetWithInput(Map<String, Object> input, Extension ext) {
    if (top != -1) {
      throw new IllegalStateException(
          "Reset visitor state while stack is not clear: " + Arrays.toString(stack));
    }
    clear();

    this.symtab.putAll(STATIC_OBJECTS); // support static method calling
    this.symtab.put("params", SelParams.of(input, ext));

    this.inputTab = input;
  }

  boolean isStackEmpty() {
    return top == -1;
  }

  void push(SelType obj) {
    stack[++top] = obj;
  }

  SelType pop() {
    SelType ret = stack[top];
    stack[top--] = null;
    return ret;
  }

  SelType readWithOffset(int offset) {
    return stack[top - offset];
  }

  SelType get(String key) {
    if (symtab.containsKey(key)) {
      return symtab.get(key);
    } else if (inputTab != null && inputTab.containsKey(key)) {
      Object inputVal = inputTab.get(key);
      SelType newVal = box(inputVal);
      symtab.put(key, newVal);
      return newVal;
    } else {
      return SelType.NULL;
    }
  }

  void createIfMissing(String key, SelTypes type) { // won't create array
    if (symtab.containsKey(key)) {
      return;
    } else if (inputTab != null && inputTab.containsKey(key)) {
      Object inputVal = inputTab.get(key);
      SelType newVal = box(inputVal);
      symtab.put(key, newVal);
    } else {
      symtab.put(key, type.newSelTypeObj());
    }
  }

  void put(String key, SelType obj) {
    symtab.put(key, obj);
  }
}
