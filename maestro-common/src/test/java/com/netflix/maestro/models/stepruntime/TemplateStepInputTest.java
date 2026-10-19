/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.models.stepruntime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.parameter.ParamType;
import java.util.List;
import org.junit.Test;

public class TemplateStepInputTest extends MaestroBaseTest {
  @Test
  public void testDeserializePositionWithOutputs() throws Exception {
    TemplateStepInput position =
        MAPPER.readValue(
            "{\"successors\":[\"audit\"],\"params\":[\"table\"],"
                + "\"outputs\":{\"row_count\":{\"type\":\"LONG\"}}}",
            TemplateStepInput.class);
    assertNull(position.getPredecessors());
    assertEquals(List.of("audit"), position.getSuccessors());
    assertEquals(List.of("table"), position.getParams());
    assertTrue(position.hasOutputs());
    assertEquals(ParamType.LONG, position.getOutputs().get("row_count").getType());
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    TemplateStepInput position =
        MAPPER.readValue(
            "{\"predecessors\":[\"publish\"],\"params\":[\"table\",\"snapshot\"]}",
            TemplateStepInput.class);
    assertFalse(position.hasOutputs());
    String ser1 = MAPPER.writeValueAsString(position);
    assertEquals("{\"predecessors\":[\"publish\"],\"params\":[\"table\",\"snapshot\"]}", ser1);
    TemplateStepInput actual = MAPPER.readValue(ser1, TemplateStepInput.class);
    assertEquals(position, actual);
    assertEquals(ser1, MAPPER.writeValueAsString(actual));
  }
}
