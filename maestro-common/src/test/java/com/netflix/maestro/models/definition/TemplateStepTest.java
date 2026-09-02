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
package com.netflix.maestro.models.definition;

import com.netflix.maestro.MaestroBaseTest;
import org.junit.Assert;
import org.junit.Test;

public class TemplateStepTest extends MaestroBaseTest {
  @Test
  public void testSerde() throws Exception {
    TemplateStep def =
        (TemplateStep) loadObject("fixtures/typedsteps/sample-template-step.json", Step.class);
    Assert.assertNotNull(def);
    Assert.assertEquals(StepType.TEMPLATE, def.getType());
    Assert.assertEquals("publish", def.getId());
    Assert.assertEquals("write_audit_publish", def.getSubType());
    Assert.assertEquals("v3", def.getSubTypeVersion());
    Assert.assertEquals(2, def.getParams().size());
    TemplateStep def2 = (TemplateStep) MAPPER.readValue(MAPPER.writeValueAsString(def), Step.class);
    Assert.assertEquals(def, def2);
  }
}
