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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class SelMapTest {

  private SelMap orig;

  @Before
  public void setUp() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("foo", "bar");
    map.put("num", 123L);
    orig = SelMap.of(map);
  }

  @Test
  public void testAssignOp() {
    SelMap cur = SelMap.of(null);
    cur.assignOps(SelOp.ASSIGN, orig);
    assertEquals("MAP: {foo=bar, num=123}", cur.type() + ": " + cur.toString());
    orig.getInternalVal().put(SelString.of("foo"), SelString.of("baz"));
    assertEquals("MAP: {foo=baz, num=123}", cur.type() + ": " + cur.toString());
    cur.assignOps(SelOp.ASSIGN, cur);
    assertEquals("MAP: {foo=baz, num=123}", cur.type() + ": " + cur.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMismatchAssignOp() {
    SelMap cur = SelMap.of(null);
    cur.assignOps(SelOp.ASSIGN, SelString.of("abc"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidAssignOp() {
    SelMap cur = SelMap.of(new HashMap<>());
    cur.assignOps(SelOp.ADD_ASSIGN, orig);
  }

  @Test
  public void testUnbox() {
    Map<String, Object> map = orig.unbox();
    assertEquals("{foo=bar, num=123}", String.valueOf(map));
  }

  @Test
  public void testUnboxNull() {
    Map<String, Object> map = SelMap.of(null).unbox();
    assertNull(map);
  }

  @Test
  public void testCallSize() {
    SelType res = orig.call("size", new SelType[] {});
    assertEquals("LONG: 2", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallContainsKey() {
    SelType res = orig.call("containsKey", new SelType[] {SelString.of("num")});
    assertEquals("BOOLEAN: true", res.type() + ": " + res.toString());
    res = orig.call("containsKey", new SelType[] {SelString.of("not-existing")});
    assertEquals("BOOLEAN: false", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallGet() {
    SelType res = orig.call("get", new SelType[] {SelString.of("num")});
    assertEquals("LONG: 123", res.type() + ": " + res.toString());
    res = orig.call("get", new SelType[] {SelString.of("fuu")});
    assertEquals("NULL: NULL", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallGetOrDefault() {
    SelType res = orig.call("getOrDefault", new SelType[] {SelString.of("num"), SelLong.of(1L)});
    assertEquals("LONG: 123", res.type() + ": " + res.toString());
    res = orig.call("getOrDefault", new SelType[] {SelString.of("fuu"), SelLong.of(1L)});
    assertEquals("LONG: 1", res.type() + ": " + res.toString());
  }

  @Test(expected = ClassCastException.class)
  public void testCallGetNullKey() {
    orig.call("get", new SelType[] {SelType.NULL});
  }

  @Test(expected = ClassCastException.class)
  public void testCallGetInvalidTypeKey() {
    orig.call("get", new SelType[] {SelLong.of(1)});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallGetInvalidArgs() {
    orig.call("get", new SelType[] {});
  }

  @Test
  public void testCallPut() {
    SelType res = orig.call("put", new SelType[] {SelString.of("foo"), SelBoolean.of(true)});
    assertEquals("STRING: bar", res.type() + ": " + res.toString());
    res = orig.call("get", new SelType[] {SelString.of("foo")});
    assertEquals("BOOLEAN: true", res.type() + ": " + res.toString());
    res = orig.call("put", new SelType[] {SelString.of("foo"), SelType.NULL});
    assertEquals("BOOLEAN: true", res.type() + ": " + res.toString());
    res = orig.call("get", new SelType[] {SelString.of("foo")});
    assertEquals("NULL: NULL", res.type() + ": " + res.toString());
    res = orig.call("put", new SelType[] {SelString.of("fuu"), SelString.of("bat")});
    assertEquals(SelType.NULL, res);
    res = orig.call("get", new SelType[] {SelString.of("fuu")});
    assertEquals("STRING: bat", res.type() + ": " + res.toString());
  }

  @Test(expected = ClassCastException.class)
  public void testCallPutInvalidTypeKey() {
    orig.call("put", new SelType[] {SelLong.of(1), SelLong.of(1)});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCallPutInvalidArgs() {
    orig.call("put", new SelType[] {SelLong.of(1)});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidTypeNULL() {
    orig.call("put", new SelType[] {SelString.of("foo"), SelType.NULL});
    orig.unbox();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidTypeNull() {
    orig.call("put", new SelType[] {SelString.of("foo"), null});
    orig.unbox();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTypeNullValue() {
    SelMap.of(Collections.singletonMap("bar", null));
  }
}
