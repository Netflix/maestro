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
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.stepruntime.JobTemplate;
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

  private static JobTemplate.Definition definition(StepType stepType, List<Step> steps) {
    JobTemplate.Definition definition = new JobTemplate.Definition();
    definition.setJobType("test-job");
    definition.setStepType(stepType);
    definition.setSteps(steps);
    return definition;
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

  private ConstraintViolation<TestDefinition> singleViolation(JobTemplate.Definition definition) {
    Set<ConstraintViolation<TestDefinition>> violations =
        validator.validate(new TestDefinition(definition));
    assertEquals(1, violations.size());
    ConstraintViolation<TestDefinition> violation = violations.iterator().next();
    assertEquals("definition.steps", violation.getPropertyPath().toString());
    return violation;
  }

  @Test
  public void isNull() {
    assertTrue(validator.validate(new TestDefinition(null)).isEmpty());
  }

  @Test
  public void isValidWithoutStepsForNonTemplateStepType() {
    assertTrue(
        validator.validate(new TestDefinition(definition(StepType.NOTEBOOK, null))).isEmpty());
  }

  @Test
  public void isValidWithStepsForTemplateStepType() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, List.of(step("write", "audit"), step("audit", null)));
    assertTrue(validator.validate(new TestDefinition(definition)).isEmpty());
  }

  @Test
  public void isStepsRejectedForNonTemplateStepType() {
    JobTemplate.Definition definition =
        definition(StepType.KUBERNETES, List.of(step("write", null)));
    assertEquals(
        "[job template step definitions] can only be set for the template step type "
            + "- rejected step type is [KUBERNETES]",
        singleViolation(definition).getMessage());
  }

  @Test
  public void isStepsNullForTemplateStepType() {
    assertEquals(
        "[job template step definitions] cannot be null or empty for the template step type",
        singleViolation(definition(StepType.TEMPLATE, null)).getMessage());
  }

  @Test
  public void isStepsEmptyForTemplateStepType() {
    assertEquals(
        "[job template step definitions] cannot be null or empty for the template step type",
        singleViolation(definition(StepType.TEMPLATE, new ArrayList<>())).getMessage());
  }

  @Test
  public void isStepIdDuplicate() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, List.of(step("write", null), step("write", null)));
    assertEquals(
        "[workflow step definitions] contain duplicate step ids",
        singleViolation(definition).getMessage());
  }

  @Test
  public void isTransitionToMissingStep() {
    JobTemplate.Definition definition =
        definition(StepType.TEMPLATE, List.of(step("write", "audit")));
    assertEquals(
        "[workflow step transition] is invalid for step [write]'s successor step id [audit], "
            + "which does not exist in steps",
        singleViolation(definition).getMessage());
  }

  @Test
  public void isStepListSizeTooLarge() {
    JobTemplate.Definition definition =
        definition(
            StepType.TEMPLATE,
            Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT + 1, step("write", null)));
    assertEquals(
        String.format(
            "[workflow step list] is too large [%s] and over the size limit [%s]",
            Constants.STEP_LIST_SIZE_LIMIT + 1, Constants.STEP_LIST_SIZE_LIMIT),
        singleViolation(definition).getMessage());
  }
}
