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
import java.util.List;
import java.util.Map;
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
    // attached steps are keyed by the position the template declares
    Assert.assertEquals(
        List.of("writes", "after_publish"), List.copyOf(def.getStepInputs().keySet()));
    List<Step> writes = def.getStepInputs().get("writes");
    Assert.assertEquals(List.of("write_a", "write_b"), writes.stream().map(Step::getId).toList());
    Assert.assertEquals(
        Map.of("write_b", "true"), writes.getFirst().getTransition().getSuccessors());
    Assert.assertTrue(writes.getLast().getTransition().getSuccessors().isEmpty());
    Assert.assertEquals(0L, (long) writes.getLast().getParams().get("row_count").getValue());
    List<Step> afterPublish = def.getStepInputs().get("after_publish");
    Assert.assertEquals("announce", afterPublish.getFirst().getId());
    Assert.assertEquals(
        "'published ' + published_snapshot;",
        afterPublish.getFirst().getParams().get("msg").getExpression());
    TemplateStep def2 = (TemplateStep) MAPPER.readValue(MAPPER.writeValueAsString(def), Step.class);
    Assert.assertEquals(def, def2);
  }
}
