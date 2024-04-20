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
package com.netflix.sel.visitor;

import static org.junit.Assert.*;

import com.netflix.sel.type.SelString;
import com.netflix.sel.type.SelType;
import com.netflix.sel.type.SelTypes;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class SelVisitorStateTest {

  private SelVisitorState state;
  private Map<String, Object> params;

  @Before
  public void setUp() throws Exception {
    state = new SelVisitorState(2);
    params = new HashMap<>();
    params.put("foo", "bar");
  }

  @Test
  public void testClear() {
    assertTrue(state.isStackEmpty());
    state.push(SelString.of("foo"));
    assertFalse(state.isStackEmpty());
    state.clear();
    assertTrue(state.isStackEmpty());
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidResetWithInput() {
    assertTrue(state.isStackEmpty());
    state.push(SelString.of("foo"));
    state.resetWithInput(new HashMap<>(), null);
  }

  @Test
  public void testResetWithInputAndGet() {
    assertTrue(state.isStackEmpty());
    assertEquals(SelType.NULL, state.get("foo"));
    state.resetWithInput(params, null);
    assertEquals(SelString.of("bar"), state.get("foo"));
    assertEquals(SelType.NULL, state.get("bar"));
  }

  @Test
  public void testPush() {
    assertTrue(state.isStackEmpty());
    state.push(SelString.of("foo"));
    SelType res = state.readWithOffset(0);
    assertEquals("STRING: foo", res.type() + ": " + res);
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testPushStackOverflow() {
    state.push(SelString.of("foo"));
    state.push(SelString.of("foo"));
    state.push(SelString.of("foo"));
  }

  @Test
  public void testPop() {
    assertTrue(state.isStackEmpty());
    state.push(SelString.of("foo"));
    state.push(SelString.of("bar"));
    assertFalse(state.isStackEmpty());
    SelType res = state.pop();
    assertEquals("STRING: bar", res.type() + ": " + res);
    res = state.pop();
    assertEquals("STRING: foo", res.type() + ": " + res);
    assertTrue(state.isStackEmpty());
  }

  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testInvalidPop() {
    state.pop();
  }

  @Test
  public void createIfMissing() {
    state.resetWithInput(params, null);
    state.createIfMissing("foo", SelTypes.STRING);
    SelType res = state.get("foo");
    assertEquals("STRING: bar", res.type() + ": " + res);
    state.createIfMissing("fuu", SelTypes.STRING);
    res = state.get("fuu");
    assertEquals("STRING: null", res.type() + ": " + res);
  }

  @Test
  public void put() {
    state.put("foo", SelString.of("bar"));
    SelType res = state.get("foo");
    assertEquals("STRING: bar", res.type() + ": " + res);
  }

  @Test
  public void testPutAnExistingKey() {
    state.resetWithInput(params, null);
    state.put("foo", SelString.of("bat"));
    SelType res = state.get("foo");
    assertEquals("STRING: bat", res.type() + ": " + res);
  }
}
