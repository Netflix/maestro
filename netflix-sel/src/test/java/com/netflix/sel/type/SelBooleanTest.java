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

public class SelBooleanTest {

  private SelBoolean one;
  private SelBoolean another;

  @Before
  public void setUp() throws Exception {
    one = SelBoolean.of(true);
    another = SelBoolean.of(false);
  }

  @Test
  public void testAssignOps() {
    SelBoolean res = one.assignOps(SelOp.ASSIGN, another);
    another.assignOps(SelOp.ASSIGN, SelBoolean.of(true));
    assertEquals("BOOLEAN: false", one.type() + ": " + one);
    assertEquals("BOOLEAN: true", another.type() + ": " + another);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAssignOp() {
    one.assignOps(SelOp.ASSIGN, SelString.of("foo"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOpType() {
    one.assignOps(SelOp.EQUAL, one);
  }

  @Test
  public void testBinaryOps() {
    assertFalse(one.binaryOps(SelOp.AND, another).booleanVal());
    assertTrue(one.binaryOps(SelOp.OR, another).booleanVal());
    assertFalse(one.binaryOps(SelOp.EQUAL, another).booleanVal());
    assertTrue(one.binaryOps(SelOp.NOT_EQUAL, another).booleanVal());
    assertFalse(one.binaryOps(SelOp.NOT, another).booleanVal());
    assertFalse(one.binaryOps(SelOp.EQUAL, SelType.NULL).booleanVal());
    assertTrue(one.binaryOps(SelOp.NOT_EQUAL, SelType.NULL).booleanVal());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBinaryOpType() {
    one.binaryOps(SelOp.ADD, SelString.of("foo"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidBinaryOp() {
    one.binaryOps(SelOp.ADD, SelBoolean.of(false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBinaryOpRhs() {
    one.binaryOps(SelOp.ADD, SelType.NULL);
  }
}
