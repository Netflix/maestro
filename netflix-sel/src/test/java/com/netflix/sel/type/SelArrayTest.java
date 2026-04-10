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

import com.netflix.sel.visitor.SelOp;
import org.junit.Before;
import org.junit.Test;

public class SelArrayTest {

  private SelArray one;
  private SelArray another;

  @Before
  public void setUp() throws Exception {
    one = SelArray.of(new String[] {"foo", "bar"}, SelTypes.STRING_ARRAY);
    another = SelArray.of(new long[] {1, 2, 3}, SelTypes.LONG_ARRAY);
  }

  @Test
  public void testSet() {
    assertEquals("STRING_ARRAY: [foo, bar]", one.type() + ": " + one);
    one.set(1, SelString.of("baz"));
    assertEquals("STRING_ARRAY: [foo, baz]", one.type() + ": " + one);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSet() {
    another.set(1, SelString.of("baz"));
  }

  @Test
  public void testGet() {
    assertEquals("STRING: bar", one.get(1).type() + ": " + one.get(1));
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testInvalidGet() {
    one.get(2);
  }

  @Test
  public void testAssignOps() {
    one.assignOps(SelOp.ASSIGN, new SelArray(1, SelTypes.STRING_ARRAY));
    assertEquals("STRING_ARRAY: [null]", one.type() + ": " + one);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAssignType() {
    one.assignOps(SelOp.ASSIGN, another);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOp() {
    one.assignOps(SelOp.ADD_ASSIGN, another);
  }

  @Test
  public void unbox() {
    assertArrayEquals(new String[] {"foo", "bar"}, (String[]) one.unbox());
    assertArrayEquals(new long[] {1, 2, 3}, (long[]) another.unbox());
  }

  @Test
  public void field() {
    SelLong res = one.field(SelString.of("length"));
    assertEquals(2, res.longVal());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void invalidField() {
    one.field(SelString.of("size"));
  }

  @Test
  public void testContainsTrue() {
    SelBoolean result = (SelBoolean) one.call("contains", new SelType[] {SelString.of("foo")});
    assertTrue(result.booleanVal());
  }

  @Test
  public void testContainsFalse() {
    SelBoolean result = (SelBoolean) one.call("contains", new SelType[] {SelString.of("baz")});
    assertFalse(result.booleanVal());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testContainsInvalidArgs() {
    one.call("contains", new SelType[] {});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidMethod() {
    one.call("invalidMethod", new SelType[] {SelString.of("test")});
  }
}
