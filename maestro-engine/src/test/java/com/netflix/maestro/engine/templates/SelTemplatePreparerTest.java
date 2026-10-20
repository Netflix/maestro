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
package com.netflix.maestro.engine.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import org.junit.Test;

public class SelTemplatePreparerTest extends MaestroBaseTest {
  private final SelTemplatePreparer preparer = new SelTemplatePreparer();

  @Test
  public void testType() {
    assertEquals(TemplatePrepare.SEL, preparer.type());
  }

  @Test
  public void testInjectParamsAddsNothing() throws Exception {
    JobTemplate.Definition definition =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class)
            .getDefinition();
    assertTrue(preparer.injectParams(definition).isEmpty());
  }

  @Test
  public void testPrepareIsNotImplemented() throws Exception {
    JobTemplate.Definition definition =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class)
            .getDefinition();
    TemplatePrepare prepare = new TemplatePrepare();
    prepare.setType(TemplatePrepare.SEL);
    prepare.setExpression("return buildExpansion(target_table);");
    definition.setPrepare(prepare);
    TemplateStep step = new TemplateStep();
    step.setId("publish");
    PrepareContext context =
        new PrepareContext(
            new WorkflowSummary(),
            step,
            StepRuntimeSummary.builder().build(),
            definition,
            new TemplateArtifact());

    AssertHelper.assertThrows(
        "the sel preparer is not implemented",
        MaestroInternalError.class,
        "Template step [publish] uses the sel preparer, which is not implemented yet",
        () -> preparer.prepare(context));
  }
}
