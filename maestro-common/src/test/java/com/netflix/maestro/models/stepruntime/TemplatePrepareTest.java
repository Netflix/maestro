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
import static org.junit.Assert.assertNull;

import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.models.definition.StepType;
import org.junit.Test;

public class TemplatePrepareTest extends MaestroBaseTest {
  @Test
  public void testDeserializeStepPreparer() throws Exception {
    JobTemplate template =
        loadObject("fixtures/stepruntime/job_template_with_step_preparer.json", JobTemplate.class);
    TemplatePrepare prepare = template.getDefinition().getPrepare();
    assertEquals(TemplatePrepare.STEP, prepare.getType());
    assertNull(prepare.getSteps());
    assertNull(prepare.getExpression());
    assertEquals("generate", prepare.getStep().getId());
    assertEquals(StepType.NOOP, prepare.getStep().getType());
    assertEquals(
        "audit__row_count;",
        template.getDefinition().getOutputParams().get("row_count").getExpression());
  }

  @Test
  public void testRoundTripSerde() throws Exception {
    TemplatePrepare prepare =
        MAPPER.readValue(
            "{\"type\":\"sel\",\"expression\":\"return buildExpansion(target_table);\"}",
            TemplatePrepare.class);
    assertEquals(TemplatePrepare.SEL, prepare.getType());
    assertEquals("return buildExpansion(target_table);", prepare.getExpression());
    String ser1 = MAPPER.writeValueAsString(prepare);
    TemplatePrepare actual = MAPPER.readValue(ser1, TemplatePrepare.class);
    assertEquals(prepare, actual);
    assertEquals(ser1, MAPPER.writeValueAsString(actual));
  }
}
