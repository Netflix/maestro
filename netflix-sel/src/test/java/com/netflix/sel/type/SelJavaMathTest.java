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

import static org.junit.Assert.*;

import org.junit.Test;

public class SelJavaMathTest {

  @Test
  public void call() {
    SelType res = SelJavaMath.INSTANCE.call("min", new SelType[] {SelLong.of(1), SelLong.of(2)});
    assertEquals("LONG: 1", res.type() + ": " + res);
    res = SelJavaMath.INSTANCE.call("max", new SelType[] {SelLong.of(1), SelLong.of(2)});
    assertEquals("LONG: 2", res.type() + ": " + res);
    res = SelJavaMath.INSTANCE.call("pow", new SelType[] {SelLong.of(3), SelLong.of(2)});
    assertEquals(SelTypes.DOUBLE, res.type());
    assertEquals(9.0, ((SelDouble) res).doubleVal(), 0.01);
    res = SelJavaMath.INSTANCE.call("random", new SelType[] {});
    assertEquals(SelTypes.DOUBLE, res.type());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallArg() {
    SelJavaMath.INSTANCE.call("random", new SelType[] {SelType.NULL});
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidCallArgType() {
    SelJavaMath.INSTANCE.call("min", new SelType[] {SelType.NULL, SelType.NULL});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallMethod() {
    SelJavaMath.INSTANCE.call("invalid", new SelType[] {});
  }
}
