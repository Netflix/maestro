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
package com.netflix.sel;

import com.netflix.sel.type.AbstractSelType;
import org.joda.time.DateTime;

public class MockType extends AbstractSelType {
  public static void staticNoArg() {}

  public String noArg() {
    return "noArg";
  }

  public static int staticOneArg(int arg) {
    return arg;
  }

  public String oneArg(String arg) {
    return arg;
  }

  public static double staticTwoArgs(double arg, boolean b) {
    return arg + (b ? 1 : 2);
  }

  public String twoArgs(String arg, DateTime dt) {
    return arg + dt;
  }
}
