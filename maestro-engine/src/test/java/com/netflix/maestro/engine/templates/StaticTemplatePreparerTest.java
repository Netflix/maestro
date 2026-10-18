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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.AssertHelper;
import com.netflix.maestro.MaestroBaseTest;
import com.netflix.maestro.engine.execution.StepRuntimeSummary;
import com.netflix.maestro.engine.execution.WorkflowSummary;
import com.netflix.maestro.models.artifact.TemplateArtifact;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StaticTemplatePreparerTest extends MaestroBaseTest {
  private final StaticTemplatePreparer preparer = new StaticTemplatePreparer();
  private JobTemplate.Definition definition;
  private PrepareContext context;

  @Before
  public void setUp() throws Exception {
    definition =
        loadObject("fixtures/stepruntime/job_template_with_steps.json", JobTemplate.class)
            .getDefinition();
    TemplateStep step = new TemplateStep();
    step.setId("publish");
    TemplateArtifact artifact = new TemplateArtifact();
    artifact.setTemplateVersion("v3");
    context =
        new PrepareContext(
            new WorkflowSummary(),
            step,
            StepRuntimeSummary.builder().build(),
            definition,
            artifact);
  }

  @Test
  public void testType() {
    assertEquals(TemplatePrepare.STATIC, preparer.type());
    assertTrue(preparer.injectParams(definition).isEmpty());
  }

  @Test
  public void testPrepareIsReadyWithRegisteredSteps() {
    PrepareResult result = preparer.prepare(context);
    assertTrue(result.isReady());
    assertEquals(
        List.of("write", "audit", "publish_snapshot"),
        result.expansion().getSteps().stream().map(Step::getId).toList());
    assertNull(result.expansion().getParams());
  }

  @Test
  public void testPrepareWithoutSteps() {
    definition.getPrepare().setSteps(null);
    AssertHelper.assertThrows(
        "static preparer needs steps",
        NullPointerException.class,
        "Job template [write_audit_publish][v3] does not define any steps for the template step [publish]",
        () -> preparer.prepare(context));
  }
}
