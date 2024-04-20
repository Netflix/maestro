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

/** Enum for all supported operators */
public enum SelOp {
  TERNARY("?:"),

  AND("&&"),
  OR("||"),
  EQUAL("=="),
  NOT_EQUAL("!="),
  LT("<"),
  GT(">"),
  LTE("<="),
  GTE(">="),
  ADD("+"),
  SUB("-"),
  MUL("*"),
  DIV("/"),
  MOD("%"),

  PLUS("+"),
  MINUS("-"),
  NOT("!"),

  ASSIGN("="),
  MUL_ASSIGN("*="),
  DIV_ASSIGN("/="),
  MOD_ASSIGN("%="),
  ADD_ASSIGN("+="),
  SUB_ASSIGN("-=");

  private final String op;

  SelOp(String op) {
    this.op = op;
  }

  @Override
  public String toString() {
    return op;
  }
}
