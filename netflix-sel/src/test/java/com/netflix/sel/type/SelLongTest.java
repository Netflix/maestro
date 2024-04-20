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

public class SelLongTest {

  private SelLong orig;

  @Before
  public void setUp() throws Exception {
    orig = SelLong.of(1);
  }

  @Test
  public void testAssignOps() {
    SelType obj = SelLong.of(2);
    SelType res = orig.assignOps(SelOp.ASSIGN, obj);
    assertEquals("LONG: 2", res.type() + ": " + res);
    res = orig.assignOps(SelOp.ASSIGN, SelLong.of(3));
    assertEquals("LONG: 2", obj.type() + ": " + obj);
    assertEquals("LONG: 3", res.type() + ": " + res);
    res = orig.assignOps(SelOp.ADD_ASSIGN, obj);
    assertEquals("LONG: 5", res.type() + ": " + res);
    res = orig.assignOps(SelOp.SUB_ASSIGN, obj);
    assertEquals("LONG: 3", res.type() + ": " + res);
    res = orig.assignOps(SelOp.MUL_ASSIGN, obj);
    assertEquals("LONG: 6", res.type() + ": " + res);
    res = orig.assignOps(SelOp.DIV_ASSIGN, obj);
    assertEquals("LONG: 3", res.type() + ": " + res);
    res = orig.assignOps(SelOp.MOD_ASSIGN, obj);
    assertEquals("LONG: 1", res.type() + ": " + res);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAssignOp() {
    orig.assignOps(SelOp.ASSIGN, SelString.of("foo"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOpType() {
    orig.assignOps(SelOp.EQUAL, orig);
  }

  @Test
  public void testBinaryOps() {
    SelType obj = SelLong.of(2);
    SelType res = orig.binaryOps(SelOp.EQUAL, obj);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.NOT_EQUAL, obj);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.LT, obj);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.GT, obj);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.LTE, obj);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.GTE, obj);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.ADD, obj);
    assertEquals("LONG: 3", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.SUB, obj);
    assertEquals("LONG: -1", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.MUL, obj);
    assertEquals("LONG: 2", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.DIV, obj);
    assertEquals("LONG: 0", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.MOD, obj);
    assertEquals("LONG: 1", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.PLUS, obj);
    assertEquals("LONG: 1", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.MINUS, obj);
    assertEquals("LONG: -1", res.type() + ": " + res);

    res = orig.binaryOps(SelOp.EQUAL, SelType.NULL);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.NOT_EQUAL, SelType.NULL);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);

    res = orig.binaryOps(SelOp.ADD, SelDouble.of(2.0));
    assertEquals("DOUBLE: 3.0", res.type() + ": " + res);
    res = orig.binaryOps(SelOp.ADD, SelString.of("2"));
    assertEquals("STRING: 12", res.type() + ": " + res);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBinaryOp() {
    orig.binaryOps(SelOp.EQUAL, SelBoolean.of(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBinaryOpType() {
    orig.binaryOps(SelOp.ADD, SelType.NULL);
  }

  @Test
  public void testCalls() {
    SelType res = orig.call("toString", new SelType[0]);
    assertEquals("STRING: 1", res.type() + ": " + res);
    res = orig.call("toString", new SelType[] {SelLong.of(12)});
    assertEquals("STRING: 12", res.type() + ": " + res);
    res = orig.call("valueOf", new SelType[] {SelLong.of(123)});
    assertEquals("LONG: 123", res.type() + ": " + res);
    res = orig.call("valueOf", new SelType[] {SelString.of("123")});
    assertEquals("LONG: 123", res.type() + ": " + res);
    res = orig.call("parseLong", new SelType[] {SelString.of("123")});
    assertEquals("LONG: 123", res.type() + ": " + res);
    res = orig.call("intValue", new SelType[0]);
    assertEquals("LONG: 1", res.type() + ": " + res);
  }

  @Test(expected = NumberFormatException.class)
  public void testInvalidCall() {
    orig.call("valueOf", new SelType[] {SelLong.of("foo")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNotSupportedCall() {
    orig.call("parse", new SelType[] {SelLong.of(1)});
  }
}
