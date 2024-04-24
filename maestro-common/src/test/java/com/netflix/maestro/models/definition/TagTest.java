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
import static org.junit.Assert.assertNotNull;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Test;

public class TagTest extends MaestroBaseTest {

  @Test
  public void testRoundTripSerde() throws Exception {
    Tag tag =
        loadObject("fixtures/workflows/definition/sample-tag-with-attributes.json", Tag.class);
    assertEquals(tag, MAPPER.readValue(MAPPER.writeValueAsString(tag), Tag.class));
  }

  @Test
  public void testAttributes() throws Exception {
    Tag tag =
        loadObject("fixtures/workflows/definition/sample-tag-with-attributes.json", Tag.class);
    assertNotNull(tag.getAttributes());
    assertEquals(4, tag.getAttributes().size());
    assertEquals("maestro-dev", tag.getAttributes().get("creator"));
    assertEquals("maestro-dev", tag.getAttributes().get("technical_support"));
    assertEquals(1234L, tag.getAttributes().get("created_at_milli"));
  }
}
