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
import org.junit.Test;

public class SelStringTest {

  @Test
  public void testCreateValid() {
    SelString str = SelString.of("foo");
    SelString res = SelString.create(new SelType[] {str});
    assertEquals(str.getInternalVal(), res.getInternalVal());
    res = SelString.create(new SelType[] {});
    assertEquals("", res.getInternalVal());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInValid() {
    SelString.create(new SelType[] {SelString.of("foo"), SelString.of("bar")});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInValidType() {
    SelString.create(new SelType[] {SelDouble.of("123.2")});
  }

  @Test
  public void testSupportedNoArgCalls() {
    String[] methods = new String[] {"toUpperCase", "toLowerCase", "length", "isEmpty", "trim"};
    String[] expected =
        new String[] {
          "STRING: FOOBAR ", "STRING: foobar ", "LONG: 7", "BOOLEAN: false", "STRING: FooBar"
        };
    for (int i = 0; i < methods.length; ++i) {
      SelType res = SelString.of("FooBar ").call(methods[i], new SelType[0]);
      assertEquals(expected[i], res.type() + ": " + res.toString());
    }
  }

  @Test
  public void testSupportedOneArgCalls() {
    String[] methods =
        new String[] {"lastIndexOf", "equals", "compareTo", "startsWith", "endsWith", "contains"};
    String[] expected =
        new String[] {
          "LONG: 2",
          "BOOLEAN: false",
          "LONG: -41",
          "BOOLEAN: false",
          "BOOLEAN: false",
          "BOOLEAN: true"
        };
    for (int i = 0; i < methods.length; ++i) {
      SelType res = SelString.of("FooBar ").call(methods[i], new SelType[] {SelString.of("oBa")});
      assertEquals(expected[i], res.type() + ": " + res.toString());
    }
  }

  @Test
  public void testSupportedCallReplaceAll() {
    SelType res =
        SelString.of("a-B- c")
            .call("replaceAll", new SelType[] {SelString.of(" b"), SelString.of("-B-")});
    assertEquals(SelTypes.STRING, res.type());
    assertEquals("a-B- c", ((SelString) res).getInternalVal());
  }

  @Test
  public void testSupportedCallReplace() {
    SelType res =
        SelString.of("a-B- c")
            .call("replace", new SelType[] {SelString.of(" b"), SelString.of("-B-")});
    assertEquals(SelTypes.STRING, res.type());
    assertEquals("a-B- c", ((SelString) res).getInternalVal());
  }

  @Test
  public void testSupportedStaticCalls() {
    SelType res = SelString.of(null).call("valueOf", new SelType[] {SelString.of("abc")});
    assertEquals("STRING: abc", res.type() + ": " + res.toString());
    res = SelString.of(null).call("valueOf", new SelType[] {SelLong.of(123)});
    assertEquals("STRING: 123", res.type() + ": " + res.toString());
    res = SelString.of(null).call("valueOf", new SelType[] {SelBoolean.of(true)});
    assertEquals("STRING: true", res.type() + ": " + res.toString());
    res = SelString.of(null).call("valueOf", new SelType[] {SelDouble.of(1.1)});
    assertEquals("STRING: 1.1", res.type() + ": " + res.toString());
    res = SelString.of(null).call("valueOf", new SelType[] {SelLong.of(Long.MAX_VALUE)});
    assertEquals("STRING: 9223372036854775807", res.type() + ": " + res.toString());
  }

  @Test
  public void testSupportedCallSplits() {
    SelType res = SelString.of("a b c").call("split", new SelType[] {SelString.of("\\ ")});
    assertEquals(SelTypes.STRING_ARRAY, res.type());
    assertEquals(3, ((SelLong) res.field(SelString.of("length"))).intVal());
    assertEquals("[a, b, c]", res.toString());

    res = SelString.of("a b c").call("split", new SelType[] {SelString.of("\\ "), SelLong.of(2)});
    assertEquals(SelTypes.STRING_ARRAY, res.type());
    assertEquals(2, ((SelLong) res.field(SelString.of("length"))).intVal());
    assertEquals("[a, b c]", res.toString());
  }

  @Test
  public void testSupportedCallSubstrings() {
    SelType res = SelString.of("a b c").call("substring", new SelType[] {SelLong.of(3)});
    assertEquals(SelTypes.STRING, res.type());
    assertEquals(" c", ((SelString) res).getInternalVal());

    res = SelString.of("a b c").call("substring", new SelType[] {SelLong.of(1), SelLong.of(3)});
    assertEquals(SelTypes.STRING, res.type());
    assertEquals(" b", ((SelString) res).getInternalVal());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidArgs() {
    SelString.of("a b c").call("substring", new SelType[] {SelLong.of(4), SelLong.of(3)});
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidCallStatic() {
    SelString.of(null).call("split", new SelType[] {SelString.of("\\ ")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidNotImplementedCall() {
    SelString.of("a b c").call("splits", new SelType[] {SelString.of("\\ ")});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidWrongArgsCall() {
    SelString.of("a b c").call("isEmpty", new SelType[] {SelString.of("\\ ")});
  }

  @Test
  public void testFormat() {
    SelType res =
        SelString.of(null)
            .call(
                "format",
                new SelType[] {SelString.of("%s-%s"), SelString.of("ab"), SelString.of("c")});
    assertEquals("STRING: ab-c", res.type() + ": " + res.toString());
    res =
        SelString.of(null)
            .call(
                "format",
                new SelType[] {SelString.of("%s-%d"), SelString.of("ab"), SelLong.of(123)});
    assertEquals("STRING: ab-123", res.type() + ": " + res.toString());
  }

  @Test
  public void testJoin() {
    SelType res =
        SelString.of(null)
            .call("join", new SelType[] {SelString.of(","), SelString.of("ab"), SelString.of("c")});
    assertEquals("STRING: ab,c", res.type() + ": " + res.toString());
    res = SelString.of(null).call("join", new SelType[] {SelString.of(",")});
    assertEquals("STRING: ", res.type() + ": " + res.toString());
    res =
        SelString.of("abc")
            .call("join", new SelType[] {SelString.of(","), SelString.of("ab"), SelString.of("c")});
    assertEquals("STRING: ab,c", res.type() + ": " + res.toString());
    res =
        SelString.of("abc")
            .call(
                "join",
                new SelType[] {
                  SelString.of(","),
                  SelArray.of(new String[] {"z", "ab", "c"}, SelTypes.STRING_ARRAY)
                });
    assertEquals("STRING: z,ab,c", res.type() + ": " + res.toString());
  }

  @Test
  public void testEscape() {
    SelType res = SelString.of(null).call("escape", new SelType[] {SelString.of("{\"foo\"}")});
    assertEquals("STRING: {\\\"foo\\\"}", res.type() + ": " + res.toString());
    res = SelString.of("{\"foo\"}").call("escape", new SelType[0]);
    assertEquals("STRING: {\\\"foo\\\"}", res.type() + ": " + res.toString());
    res = SelString.of("{\"foo\"}").call("escape", new SelType[] {SelString.of("{\"bar\"}")});
    assertEquals("STRING: {\\\"bar\\\"}", res.type() + ": " + res.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEscapeArgLengthCall() {
    SelType res =
        SelString.of("{\"foo\"}")
            .call("escape", new SelType[] {SelString.of("{\"foo\"}"), SelString.of("{\"bar\"}")});
    assertEquals("STRING: {\\\"bar\\\"}", res.type() + ": " + res.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEscapeArgTypeCall() {
    SelType res = SelString.of("{\"foo\"}").call("escape", new SelType[] {SelLong.of(123)});
    assertEquals("STRING: {\\\"bar\\\"}", res.type() + ": " + res.toString());
  }

  @Test
  public void testAssignOp() {
    SelString obj = SelString.of("foo");
    SelString res = obj.assignOps(SelOp.ASSIGN, SelString.of("bar"));
    assertEquals("bar", res.toString());
    res = obj.assignOps(SelOp.ADD_ASSIGN, SelString.of("baz"));
    assertEquals("barbaz", res.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAssignOpType() {
    SelString.of("foo").assignOps(SelOp.ASSIGN, SelLong.of(123));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOp() {
    SelString.of("foo").assignOps(SelOp.MUL_ASSIGN, SelString.of("bar"));
  }

  @Test
  public void testBinaryOps() {
    SelString obj = SelString.of("foo");
    SelType res = obj.binaryOps(SelOp.ADD, SelString.of("bar"));
    assertEquals("foobar", res.toString());
    res = obj.binaryOps(SelOp.NOT_EQUAL, SelString.of("foobar"));
    assertEquals("BOOLEAN: true", res.type() + ": " + res.toString());
    res = obj.binaryOps(SelOp.EQUAL, SelString.of("foo"));
    assertEquals("BOOLEAN: true", res.type() + ": " + res.toString());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidBinaryOp() {
    SelString.of("foo").binaryOps(SelOp.MUL, SelString.of("bar"));
  }
}
