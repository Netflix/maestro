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

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class SelParamsTest {

  private SelParams params;

  @Before
  public void setUp() throws Exception {
    Map<String, Object> map = new HashMap<>();
    map.put("key1", true);
    map.put("key2", "val");
    map.put("key3", new long[] {1, 2, 3});
    params = SelParams.of(map, null);
  }

  @Test
  public void testCallGet() {
    SelType res = params.call("get", new SelType[] {SelString.of("key1")});
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = params.call("get", new SelType[] {SelString.of("key2")});
    assertEquals("STRING: val", res.type() + ": " + res);
    res = params.call("get", new SelType[] {SelString.of("key3")});
    assertEquals("LONG_ARRAY: [1, 2, 3]", res.type() + ": " + res);
    res = params.call("get", new SelType[] {SelString.of("foo")});
    assertEquals("NULL: NULL", res.type() + ": " + res);
    res = params.call("get", new SelType[] {SelString.of("null")});
    assertEquals("NULL: NULL", res.type() + ": " + res);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallGet() {
    params.call("get", new SelType[] {});
  }

  @Test(expected = ClassCastException.class)
  public void testInvalidCallGetCast() {
    params.call("get", new SelType[] {SelLong.of(123)});
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidCallPut() {
    params.call("put", new SelType[] {SelString.of("foo"), SelString.of("bar")});
  }

  @Test
  public void testValidField() {
    SelType res = params.field(SelString.of("key1"));
    assertEquals("BOOLEAN: true", res.type() + ": " + res);
    res = params.field(SelString.of("key2"));
    assertEquals("STRING: val", res.type() + ": " + res);
    res = params.field(SelString.of("key3"));
    assertEquals("LONG_ARRAY: [1, 2, 3]", res.type() + ": " + res);
    res = params.field(SelString.of("foo"));
    assertEquals("NULL: NULL", res.type() + ": " + res);
    res = params.field(SelString.of("null"));
    assertEquals("NULL: NULL", res.type() + ": " + res);
  }
}
