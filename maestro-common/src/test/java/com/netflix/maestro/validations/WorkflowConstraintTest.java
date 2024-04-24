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
package com.netflix.maestro.validations;

import static org.junit.Assert.assertEquals;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import javax.validation.ConstraintViolation;
import org.junit.Test;

public class WorkflowConstraintTest extends BaseConstraintTest {
  private static class TestWorkflow {
    @WorkflowConstraint Workflow workflow;

    TestWorkflow(Workflow workflow) {
      this.workflow = workflow;
    }
  }

  @Test
  public void isNull() {
    Set<ConstraintViolation<TestWorkflow>> violations = validator.validate(new TestWorkflow(null));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow", violation.getPropertyPath().toString());
    assertEquals("[workflow definition] cannot be null", violation.getMessage());
  }

  @Test
  public void isStepNull() {
    Workflow workflow = Workflow.builder().steps(null).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals("[workflow step definitions] cannot be null or empty", violation.getMessage());
  }

  @Test
  public void isStepEmpty() {
    Workflow workflow = Workflow.builder().steps(new ArrayList<>()).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals("[workflow step definitions] cannot be null or empty", violation.getMessage());
  }

  @Test
  public void isStepListSizeTooLarge() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    Workflow workflow =
        Workflow.builder()
            .steps(Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT + 1, step))
            .build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step list] is too large [301] and over the size limit [300]",
        violation.getMessage());
  }

  @Test
  public void isForeachStepListSizeTooLarge() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    ForeachStep foreachStep = new ForeachStep();
    foreachStep.setId("foreach-step");
    foreachStep.setSteps(Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT + 1, step));

    Workflow workflow = Workflow.builder().steps(Collections.singletonList(foreachStep)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step list] is too large [302] and over the size limit [300]",
        violation.getMessage());
  }

  @Test
  public void isStepDuplicate() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    Workflow workflow = Workflow.builder().steps(Arrays.asList(step, step)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals("[workflow step definitions] contain duplicate step ids", violation.getMessage());
  }

  @Test
  public void isStepDuplicateIncludingForeach() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    ForeachStep foreach = new ForeachStep();
    foreach.setSteps(Collections.singletonList(step));
    Workflow workflow = Workflow.builder().steps(Arrays.asList(step, foreach)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals("[workflow step definitions] contain duplicate step ids", violation.getMessage());
  }

  @Test
  public void isLoopParamNameValid() {
    // 1. nested foreach with duplicate loop param names should be blocked
    String duplicateName = "bar";
    TypedStep step = new TypedStep();
    step.setId("foo");

    ParamDefinition innerParam =
        MapParamDefinition.builder()
            .name("inner")
            .value(
                Collections.singletonMap(
                    duplicateName, ParamDefinition.buildParamDefinition("name", 1)))
            .build();
    ForeachStep innerForeach = new ForeachStep();
    innerForeach.setId("inner-foreach");
    innerForeach.setParams(Collections.singletonMap(Constants.LOOP_PARAMS_NAME, innerParam));
    innerForeach.setSteps(Collections.singletonList(step));

    ParamDefinition outerParam =
        MapParamDefinition.builder()
            .name("outer")
            .value(
                Collections.singletonMap(
                    duplicateName, ParamDefinition.buildParamDefinition("name", 2)))
            .build();
    ForeachStep outerForeach = new ForeachStep();
    outerForeach.setId("outer-foreach");
    outerForeach.setParams(Collections.singletonMap(Constants.LOOP_PARAMS_NAME, outerParam));
    outerForeach.setSteps(Collections.singletonList(innerForeach));

    Workflow workflow = Workflow.builder().steps(Collections.singletonList(outerForeach)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step definitions] contain duplicate loop param name [bar]",
        violation.getMessage());

    // 2. non-nested foreach with duplicate loop param names should be acceptable
    outerForeach.setSteps(Collections.singletonList(step));
    // create another foreach that's independent of outerForeach
    ParamDefinition param =
        MapParamDefinition.builder()
            .name("foreach-2")
            .value(
                Collections.singletonMap(
                    duplicateName, ParamDefinition.buildParamDefinition("name", 3)))
            .build();
    ForeachStep foreach2 = new ForeachStep();
    foreach2.setId("foreach-2");
    foreach2.setParams(Collections.singletonMap(Constants.LOOP_PARAMS_NAME, param));
    TypedStep step2 = new TypedStep();
    step2.setId("foo2");
    foreach2.setSteps(Collections.singletonList(step2));
    // outerForeach and foreach2 are not nested
    workflow = Workflow.builder().steps(Arrays.asList(outerForeach, foreach2)).build();
    violations = validator.validate(new TestWorkflow(workflow));
    // verify there is no violation
    assertEquals(0, violations.size());
  }

  @Test
  public void isTransitionSuccessorValid() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    StepTransition transition = new StepTransition();
    transition.setSuccessors(Collections.singletonMap("bar", "true"));
    step.setTransition(transition);
    Workflow workflow = Workflow.builder().steps(Collections.singletonList(step)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step transition] is invalid for step [foo]'s successor step id [bar], which does not exist in steps",
        violation.getMessage());
  }

  @Test
  public void isTransitionPredecessorValid() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    StepTransition transition = new StepTransition();
    transition.setPredecessors(Collections.singletonList("bar"));
    step.setTransition(transition);
    Workflow workflow = Workflow.builder().steps(Collections.singletonList(step)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step transition] is invalid for step [foo]'s predecessors list [[bar]], which does match computed one [null] based on the DAG",
        violation.getMessage());
  }

  @Test
  public void isInstanceStepConcurrencyOverLimit() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    step.setType(StepType.SLEEP);
    Workflow workflow =
        Workflow.builder()
            .id("foo")
            .steps(Collections.singletonList(step))
            .instanceStepConcurrency(Constants.INSTANCE_STEP_CONCURRENCY_MAX_LIMIT + 1)
            .build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.instanceStepConcurrency", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow definitions] instance_step_concurrency should be positive and less than "
            + Constants.INSTANCE_STEP_CONCURRENCY_MAX_LIMIT
            + " - rejected value is ["
            + (Constants.INSTANCE_STEP_CONCURRENCY_MAX_LIMIT + 1)
            + "]",
        violation.getMessage());
  }
}
