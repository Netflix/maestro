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

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.Step;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TemplateExpansionTest extends MaestroBaseTest {
  @Test
  public void testDeserialize() throws Exception {
    TemplateExpansion expansion =
        loadObject("fixtures/stepruntime/sample-template-expansion.json", TemplateExpansion.class);
    assertEquals(
        List.of("prep", "write_jobs", "audit"),
        expansion.getSteps().stream().map(Step::getId).toList());
    assertEquals(
        Map.of("write_jobs", "true"),
        expansion.getSteps().getFirst().getTransition().getSuccessors());
    assertEquals(
        Long.valueOf(42L),
        expansion.getSteps().get(2).getParams().get("row_count").asLongParamDef().getValue());
    assertEquals("wap-3f2a", expansion.getParams().get("wap_branch").asStringParamDef().getValue());
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    TemplateExpansion expected =
        loadObject("fixtures/stepruntime/sample-template-expansion.json", TemplateExpansion.class);
    String ser1 = MAPPER.writeValueAsString(expected);
    TemplateExpansion actual = MAPPER.readValue(ser1, TemplateExpansion.class);
    assertEquals(expected, actual);
    assertEquals(ser1, MAPPER.writeValueAsString(actual));
  }
}
