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

public class SelTypesTest {

  @Test
  public void testCallConstructor() {
    SelType res = SelTypes.STRING.call("constructor", new SelType[0]);
    assertEquals(SelTypes.STRING, res.type());
    assertEquals("", res.toString());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFailedCallConstructor() {
    SelTypes.DATETIME_ZONE.call("constructor", new SelType[0]);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFailedCall() {
    SelTypes.STRING.call("nonExist", new SelType[0]);
  }

  @Test
  public void testCreateSelArray() {
    SelType res = SelTypes.LONG.newSelTypeObjArray(3);
    assertEquals(SelTypes.LONG_ARRAY, res.type());
    assertEquals("[0, 0, 0]", res.toString());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFailedCreateSelArray() {
    SelTypes.DATETIME.newSelTypeObjArray(3);
  }

  @Test
  public void testCreateEmptySelTypeObj() {
    SelType res = SelTypes.MAP.newSelTypeObj();
    assertEquals(SelTypes.MAP, res.type());
    assertEquals("null", res.toString());
  }
}
