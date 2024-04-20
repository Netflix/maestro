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

/** Error class to support throwing an error message. */
public class SelError extends AbstractSelType {

  private final String val;

  private SelError(String t) {
    super(t == null ? 0 : t.length() * 2L);
    this.val = t;
  }

  public static SelError of(String t) {
    return new SelError(t);
  }

  @Override
  public SelTypes type() {
    return SelTypes.ERROR;
  }

  @Override
  public String toString() {
    return "ERROR: " + val;
  }
}
