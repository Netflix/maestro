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
package com.netflix.maestro.validations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.StringParamDefinition;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import jakarta.validation.ConstraintViolation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class JobTemplateDefinitionConstraintTest extends BaseConstraintTest {
  private static class TestDefinition {
    @JobTemplateDefinitionConstraint JobTemplate.Definition definition;

    TestDefinition(JobTemplate.Definition definition) {
      this.definition = definition;
    }
  }

  private static JobTemplate.Definition definition(StepType stepType, TemplatePrepare prepare) {
    JobTemplate.Definition definition = new JobTemplate.Definition();
    definition.setJobType("test-job");
    definition.setStepType(stepType);
    definition.setPrepare(prepare);
    return definition;
  }

  private static TemplatePrepare staticPrepare(List<Step> steps) {
    TemplatePrepare prepare = new TemplatePrepare();
    prepare.setType(TemplatePrepare.STATIC);
    prepare.setSteps(steps);
    return prepare;
  }

  private static TemplatePrepare selPrepare(String expression) {
    TemplatePrepare prepare = new TemplatePrepare();
    prepare.setType(TemplatePrepare.SEL);
    prepare.setExpression(expression);
    return prepare;
  }

  private static TemplatePrepare stepPrepare(Step step) {
    TemplatePrepare prepare = new TemplatePrepare();
    prepare.setType(TemplatePrepare.STEP);
    prepare.setStep(step);
    return prepare;
  }

  private static TypedStep step(String id, String successor) {
    TypedStep step = new TypedStep();
    step.setId(id);
    step.setType(StepType.NOOP);
    if (successor != null) {
      StepTransition transition = new StepTransition();
      transition.setSuccessors(Map.of(successor, "true"));
      step.setTransition(transition);
    }
    return step;
  }

  private static Map<String, ParamDefinition> params(String name) {
    return Map.of(name, StringParamDefinition.builder().name(name).value("foo").build());
  }

  private ConstraintViolation<TestDefinition> singleViolation(
      JobTemplate.Definition definition, String property) {
    Set<ConstraintViolation<TestDefinition>> violations =
        validator.validate(new TestDefinition(definition));
    assertEquals(1, violations.size());
    ConstraintViolation<TestDefinition> violation = violations.iterator().next();
    assertEquals("definition." + property, violation.getPropertyPath().toString());
    return violation;
  }

  @Test
  public void isNull() {
    assertTrue(validator.validate(new TestDefinition(null)).isEmpty());
  }

  @Test
  public void isValidWithoutTemplateFieldsForNonTemplateStepType() {
    assertTrue(
        validator.validate(new TestDefinition(definition(StepType.NOTEBOOK, null))).isEmpty());
  }

  @Test
  public void isPrepareRejectedForNonTemplateStepType() {
    JobTemplate.Definition definition =
        definition(StepType.KUBERNETES, staticPrepare(List.of(step("write", null))));
    assertEquals(
        "[job template prepare] can only be set for the template step type "
            + "- rejected step type is [KUBERNETES]",
        singleViolation(definition, "prepare").getMessage());
  }

  @Test
  public void isOutputParamsRejectedForNonTemplateStepType() {
    JobTemplate.Definition definition = definition(StepType.KUBERNETES, null);
    definition.setOutputParams(params("row_count"));
    assertEquals(
        "[job template output_params] can only be set for the template step type "
            + "- rejected step type is [KUBERNETES]",
        singleViolation(definition, "output_params").getMessage());
  }

  @Test
  public void isPrepareNullForTemplateStepType() {
    assertEquals(
        "[job template prepare] cannot be null for the template step type",
        singleViolation(definition(StepType.TEMPLATE, null), "prepare").getMessage());
  }

  @Test
  public void isPrepareTypeNull() {
    TemplatePrepare prepare = new TemplatePrepare();
    assertEquals(
        "[job template prepare] type cannot be null",
        singleViolation(definition(StepType.TEMPLATE, prepare), "prepare").getMessage());
  }

  @Test
  public void isUnknownPrepareTypeAccepted() {
    TemplatePrepare prepare = new TemplatePrepare();
    prepare.setType("remote");
    assertTrue(
        validator.validate(new TestDefinition(definition(StepType.TEMPLATE, prepare))).isEmpty());
  }

  @Test
  public void isValidStaticPrepare() {
    JobTemplate.Definition definition =
        definition(
            StepType.TEMPLATE, staticPrepare(List.of(step("write", "audit"), step("audit", null))));
    assertTrue(validator.validate(new TestDefinition(definition)).isEmpty());
  }

  @Test
  public void isStaticStepsNull() {
    assertEquals(
        "[job template prepare] steps cannot be null or empty for the static preparer",
        singleViolation(definition(StepType.TEMPLATE, staticPrepare(null)), "prepare")
            .getMessage());
  }

  @Test
  public void isStaticStepsEmpty() {
    assertEquals(
        "[job template prepare] steps cannot be null or empty for the static preparer",
        singleViolation(definition(StepType.TEMPLATE, staticPrepare(new ArrayList<>())), "prepare")
            .getMessage());
  }

  @Test
  public void isStepIdDuplicate() {
    JobTemplate.Definition definition =
        definition(
            StepType.TEMPLATE, staticPrepare(List.of(step("write", null), step("write", null))));
    assertEquals(
        "[workflow step definitions] contain duplicate step ids",
        singleViolation(definition, "steps").getMessage());
  }

  @Test
  public void isTransitionToMissingStep() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, staticPrepare(List.of(step("write", "audit"))));
    assertEquals(
        "[workflow step transition] is invalid for step [write]'s successor step id [audit], "
            + "which does not exist in steps",
        singleViolation(definition, "steps").getMessage());
  }

  @Test
  public void isStepListSizeTooLarge() {
    JobTemplate.Definition definition =
        definition(
            StepType.TEMPLATE,
            staticPrepare(
                Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT + 1, step("write", null))));
    assertEquals(
        String.format(
            "[workflow step list] is too large [%s] and over the size limit [%s]",
            Constants.STEP_LIST_SIZE_LIMIT + 1, Constants.STEP_LIST_SIZE_LIMIT),
        singleViolation(definition, "steps").getMessage());
  }

  @Test
  public void isOutputParamReserved() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, staticPrepare(List.of(step("write", null))));
    definition.setOutputParams(params(Constants.TEMPLATE_EXPANSION_PARAM));
    assertEquals(
        "[job template output params] cannot use the reserved param name [maestro_expansion]",
        singleViolation(definition, "output_params").getMessage());
  }

  @Test
  public void isOutputParamCollidingWithTemplateParam() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, staticPrepare(List.of(step("write", null))));
    definition.setParams(params("row_count"));
    definition.setOutputParams(params("row_count"));
    assertEquals(
        "[job template output params] param name [row_count] collides with a template param",
        singleViolation(definition, "output_params").getMessage());
  }

  @Test
  public void isValidSelPrepare() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, selPrepare("return buildExpansion(target_table);"));
    assertTrue(validator.validate(new TestDefinition(definition)).isEmpty());
  }

  @Test
  public void isSelExpressionEmpty() {
    assertEquals(
        "[job template prepare] expression cannot be null or empty for the sel preparer",
        singleViolation(definition(StepType.TEMPLATE, selPrepare(" ")), "prepare").getMessage());
  }

  @Test
  public void isValidStepPrepare() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, stepPrepare(step("generate", null)));
    assertTrue(validator.validate(new TestDefinition(definition)).isEmpty());
  }

  @Test
  public void isStepPrepareStepNull() {
    assertEquals(
        "[job template prepare] step cannot be null for the step preparer",
        singleViolation(definition(StepType.TEMPLATE, stepPrepare(null)), "prepare").getMessage());
  }

  @Test
  public void isStepPrepareStepTemplate() {
    TemplateStep generator = new TemplateStep();
    generator.setId("generate");
    assertEquals(
        "[job template prepare] generator step cannot be a template step",
        singleViolation(definition(StepType.TEMPLATE, stepPrepare(generator)), "prepare")
            .getMessage());
  }

  @Test
  public void isStepPrepareStepDeclaringExpansion() {
    TypedStep generator = step("generate", null);
    generator.setParams(params(Constants.TEMPLATE_EXPANSION_PARAM));
    assertEquals(
        "[job template prepare] generator step cannot declare the reserved param [maestro_expansion]",
        singleViolation(definition(StepType.TEMPLATE, stepPrepare(generator)), "prepare")
            .getMessage());
  }
}
