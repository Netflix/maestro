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

import static org.junit.Assert.assertEquals;

import com.netflix.sel.visitor.SelOp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class SelMapMapTest {

  private SelMap orig;

  @Before
  public void setUp() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("foo", Collections.singletonMap("bar", 1.23));
    orig = SelMap.of(map);
  }

  @Test
  public void testAssignOp() {
    SelMap cur = SelMap.of(null);
    cur.assignOps(SelOp.ASSIGN, orig);
    assertEquals("MAP: {foo={bar=1.23}}", cur.type() + ": " + cur.toString());
    orig.getInternalVal().put(SelString.of("foo"), SelLong.of(1L));
    assertEquals("MAP: {foo=1}", cur.type() + ": " + cur.toString());
    cur.assignOps(SelOp.ASSIGN, cur);
    assertEquals("MAP: {foo=1}", cur.type() + ": " + cur.toString());
  }

  @Test
  public void testUnbox() {
    Map<String, Object> map = orig.unbox();
    assertEquals("{foo={bar=1.23}}", String.valueOf(map));
  }

  @Test
  public void testCallGet() {
    SelType res = orig.call("get", new SelType[] {SelString.of("foo")});
    assertEquals("MAP: {bar=1.23}", res.type() + ": " + res.toString());
    res = orig.call("get", new SelType[] {SelString.of("fuu")});
    assertEquals("NULL: NULL", res.type() + ": " + res.toString());
  }

  @Test
  public void testCallPut() {
    SelType res =
        orig.call(
            "put",
            new SelType[] {SelString.of("foo"), SelMap.of(Collections.singletonMap("baz", true))});
    assertEquals("MAP: {bar=1.23}", res.type() + ": " + res.toString());
    res = orig.call("get", new SelType[] {SelString.of("foo")});
    assertEquals("MAP: {baz=true}", res.type() + ": " + res.toString());
    res = orig.call("put", new SelType[] {SelString.of("foo"), SelType.NULL});
    assertEquals("MAP: {baz=true}", res.type() + ": " + res.toString());
    res = orig.call("get", new SelType[] {SelString.of("foo")});
    assertEquals("NULL: NULL", res.type() + ": " + res.toString());
    res = orig.call("put", new SelType[] {SelString.of("fuu"), SelBoolean.of(true)});
    assertEquals(SelType.NULL, res);
    res = orig.call("get", new SelType[] {SelString.of("fuu")});
    assertEquals("BOOLEAN: true", res.type() + ": " + res.toString());
  }
}
