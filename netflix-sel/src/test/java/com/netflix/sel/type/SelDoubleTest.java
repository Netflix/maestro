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

public class SelDoubleTest {
  private SelDouble one;

  @Before
  public void setUp() throws Exception {
    one = SelDouble.of(1.1);
  }

  @Test
  public void testAssignOps() {
    SelDouble obj = SelDouble.of(2.2);
    SelDouble res = one.assignOps(SelOp.ASSIGN, obj);
    assertEquals(2.2, res.doubleVal(), 0.01);
    res = one.assignOps(SelOp.ASSIGN, SelDouble.of(3.3));
    assertEquals(2.2, obj.doubleVal(), 0.01);
    assertEquals(3.3, res.doubleVal(), 0.01);
    res = one.assignOps(SelOp.ADD_ASSIGN, obj);
    assertEquals(5.5, res.doubleVal(), 0.01);
    res = one.assignOps(SelOp.SUB_ASSIGN, obj);
    assertEquals(3.3, res.doubleVal(), 0.01);
    res = one.assignOps(SelOp.MUL_ASSIGN, obj);
    assertEquals(7.26, res.doubleVal(), 0.01);
    res = one.assignOps(SelOp.DIV_ASSIGN, obj);
    assertEquals(3.3, res.doubleVal(), 0.01);
    res = one.assignOps(SelOp.MOD_ASSIGN, obj);
    assertEquals(1.1, res.doubleVal(), 0.01);
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
    SelType obj = SelLong.of(2);
    SelType res = one.binaryOps(SelOp.EQUAL, obj);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = one.binaryOps(SelOp.NOT_EQUAL, obj);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = one.binaryOps(SelOp.LT, obj);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = one.binaryOps(SelOp.GT, obj);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = one.binaryOps(SelOp.LTE, obj);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = one.binaryOps(SelOp.GTE, obj);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = one.binaryOps(SelOp.ADD, obj);
    assertEquals(3.1, ((SelDouble) res).doubleVal(), 0.01);
    res = res.binaryOps(SelOp.SUB, obj);
    assertEquals(1.1, ((SelDouble) res).doubleVal(), 0.01);
    res = res.binaryOps(SelOp.MUL, obj);
    assertEquals(2.2, ((SelDouble) res).doubleVal(), 0.01);
    res = res.binaryOps(SelOp.DIV, obj);
    assertEquals(1.1, ((SelDouble) res).doubleVal(), 0.01);
    res = res.binaryOps(SelOp.MOD, obj);
    assertEquals(1.1, ((SelDouble) res).doubleVal(), 0.01);
    res = res.binaryOps(SelOp.PLUS, obj);
    assertEquals(1.1, ((SelDouble) res).doubleVal(), 0.01);
    res = res.binaryOps(SelOp.MINUS, obj);
    assertEquals(-1.1, ((SelDouble) res).doubleVal(), 0.01);

    res = one.binaryOps(SelOp.EQUAL, SelType.NULL);
    assertEquals("BOOLEAN: false", res.type() + ": " + res);
    res = one.binaryOps(SelOp.NOT_EQUAL, SelType.NULL);
    assertEquals("BOOLEAN: true", res.type() + ": " + res);

    res = one.binaryOps(SelOp.ADD, SelString.of("2"));
    assertEquals("STRING: 1.12", res.type() + ": " + res);
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidBinaryOp() {
    one.binaryOps(SelOp.EQUAL, SelBoolean.of(true));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidBinaryOpType() {
    one.binaryOps(SelOp.ADD, SelType.NULL);
  }

  @Test
  public void testCalls() {
    SelType res = one.call("toString", new SelType[0]);
    assertEquals("STRING: 1.1", res.type() + ": " + res);
    res = one.call("toString", new SelType[] {SelDouble.of(2.2)});
    assertEquals("STRING: 2.2", res.type() + ": " + res);
    res = one.call("valueOf", new SelType[] {SelDouble.of(12.3)});
    assertEquals("DOUBLE: 12.3", res.type() + ": " + res);
    res = one.call("valueOf", new SelType[] {SelString.of("12.3")});
    assertEquals("DOUBLE: 12.3", res.type() + ": " + res);
    res = one.call("intValue", new SelType[0]);
    assertEquals("LONG: 1", res.type() + ": " + res);
    res = one.call("longValue", new SelType[0]);
    assertEquals("LONG: 1", res.type() + ": " + res);
  }

  @Test(expected = NumberFormatException.class)
  public void testInvalidCall() {
    one.call("valueOf", new SelType[] {SelLong.of("foo")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testNotSupportedCall() {
    one.call("parse", new SelType[] {SelLong.of(1)});
  }
}
