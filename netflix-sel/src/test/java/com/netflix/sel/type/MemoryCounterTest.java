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

import com.netflix.sel.MockType;
import com.netflix.sel.util.MemoryCounter;
import org.junit.After;
import org.junit.Test;

public class MemoryCounterTest {

  static class MockSelType extends AbstractSelType {
    MockSelType(String s) {
      super(s == null ? 0 : s.length() * 2L);
    }
  }

  @After
  public void tearDown() {
    MemoryCounter.setMemoryLimit(1024 * 1024);
    MemoryCounter.reset();
  }

  @Test
  public void testRecordMemoryUsage() {
    MemoryCounter.setMemoryLimit(200);
    MemoryCounter.reset();
    new MockType();
    assertEquals(32, MemoryCounter.usedMemory());
    new MockType();
    assertEquals(64, MemoryCounter.usedMemory());
    new MockSelType(null);
    assertEquals(96, MemoryCounter.usedMemory());
    new MockSelType("foobar");
    assertEquals(140, MemoryCounter.usedMemory());
    MemoryCounter.reset();
    assertEquals(0, MemoryCounter.usedMemory());
  }

  @Test(expected = IllegalStateException.class)
  public void testBeyondMemoryLimit() {
    MemoryCounter.setMemoryLimit(10);
    MemoryCounter.reset();
    new MockSelType("foobar");
    assertEquals(12, MemoryCounter.usedMemory());
  }
}
