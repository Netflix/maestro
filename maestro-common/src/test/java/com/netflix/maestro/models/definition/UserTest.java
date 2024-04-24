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
package com.netflix.maestro.models.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class UserTest extends MaestroBaseTest {
  @Test
  public void testRoundTripSerdeOnlyName() throws Exception {
    User user = loadObject("fixtures/workflows/definition/sample-user-only-name.json", User.class);
    assertEquals(user, MAPPER.readValue(MAPPER.writeValueAsString(user), User.class));
    assertEquals("demo", user.getName());
    assertNull(user.getExtraInfo());
  }

  @Test
  public void testRoundTripSerdeNested() throws Exception {
    User user = loadObject("fixtures/workflows/definition/sample-user-nested.json", User.class);
    assertEquals(user, MAPPER.readValue(MAPPER.writeValueAsString(user), User.class));
    assertEquals("demo", user.getName());
    assertEquals(3, user.getExtraInfo().size());
  }

  @Test
  public void testRoundTripSerdeNull() throws Exception {
    User user = loadObject("fixtures/workflows/definition/sample-user-nullable.json", User.class);
    assertEquals(user, MAPPER.readValue(MAPPER.writeValueAsString(user), User.class));
    assertEquals("demo", user.getName());
    assertEquals(1, user.getExtraInfo().size());
  }

  @Test
  public void testRoundTripSerdeSimple() throws Exception {
    User user = loadObject("fixtures/workflows/definition/sample-user-simple.json", User.class);
    assertEquals(user, MAPPER.readValue(MAPPER.writeValueAsString(user), User.class));
    assertEquals("demo", user.getName());
    assertEquals(0, user.getExtraInfo().size());
  }
}
