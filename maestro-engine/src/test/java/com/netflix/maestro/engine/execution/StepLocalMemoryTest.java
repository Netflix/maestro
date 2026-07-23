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
package com.netflix.maestro.engine.execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class StepLocalMemoryTest {
  private static final String UUID_1 = "uuid-1";
  private static final String UUID_2 = "uuid-2";

  @Before
  public void setUp() {
    StepLocalMemory.remove(UUID_1);
    StepLocalMemory.remove(UUID_2);
  }

  @Test
  public void testGetOrCreate() {
    Map<String, Object> memory1 = StepLocalMemory.getOrCreate(UUID_1);
    assertNotNull(memory1);
    assertTrue(memory1.isEmpty());

    memory1.put("key1", "value1");
    assertEquals("value1", StepLocalMemory.getOrCreate(UUID_1).get("key1"));
  }

  @Test
  public void testIsolation() {
    Map<String, Object> memory1 = StepLocalMemory.getOrCreate(UUID_1);
    Map<String, Object> memory2 = StepLocalMemory.getOrCreate(UUID_2);

    memory1.put("key", "value1");
    memory2.put("key", "value2");

    assertEquals("value1", StepLocalMemory.getOrCreate(UUID_1).get("key"));
    assertEquals("value2", StepLocalMemory.getOrCreate(UUID_2).get("key"));
  }

  @Test
  public void testRemove() {
    Map<String, Object> memory1 = StepLocalMemory.getOrCreate(UUID_1);
    memory1.put("key", "value");

    StepLocalMemory.remove(UUID_1);
    assertTrue(StepLocalMemory.getOrCreate(UUID_1).isEmpty());
  }

  @Test
  public void testSizeLimit() {
    Map<String, Object> memory1 = StepLocalMemory.getOrCreate(UUID_1);
    memory1.put("small", "data");

    // 64 KB is 65536 bytes. Let's create a string slightly larger than 64 KB.
    String largeString = String.join("", Collections.nCopies(66000, "a"));
    try {
      memory1.put("large", largeString);
      fail("Should have thrown IllegalArgumentException due to size limit");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Step local memory size limit exceeded"));
    }
  }
}
