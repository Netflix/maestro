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
import static org.junit.Assert.assertTrue;

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.TemplateStep;
import com.netflix.maestro.models.definition.TypedStep;
import com.netflix.maestro.models.definition.WhileStep;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  public void isLoopStepListSizeTooLarge() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    ForeachStep foreachStep = new ForeachStep();
    foreachStep.setId("foreach-step");
    foreachStep.setSteps(Collections.nCopies(Constants.STEP_LIST_SIZE_LIMIT - 10, step));
    WhileStep whileStep = new WhileStep();
    whileStep.setId("while-step");
    whileStep.setSteps(Collections.nCopies(10, step));

    Workflow workflow = Workflow.builder().steps(List.of(foreachStep, whileStep)).build();
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
  public void isStepDuplicateIncludingWhile() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    WhileStep whileStep = new WhileStep();
    whileStep.setSteps(Collections.singletonList(step));
    Workflow workflow = Workflow.builder().steps(Arrays.asList(step, whileStep)).build();
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
  public void isLoopParamNameValidWithForeachWhileLoop() {
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
    WhileStep outerLoop = new WhileStep();
    outerLoop.setId("outer-loop");
    outerLoop.setParams(Collections.singletonMap(Constants.LOOP_PARAMS_NAME, outerParam));
    outerLoop.setSteps(Collections.singletonList(innerForeach));

    Workflow workflow = Workflow.builder().steps(Collections.singletonList(outerLoop)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step definitions] contain duplicate loop param name [bar]",
        violation.getMessage());

    // 2. non-nested loop with duplicate loop param names should be acceptable
    outerLoop.setSteps(Collections.singletonList(step));
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
    workflow = Workflow.builder().steps(Arrays.asList(outerLoop, foreach2)).build();
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
  public void isStepDuplicateIncludingTemplateStepInputs() {
    TypedStep step = new TypedStep();
    step.setId("foo");
    TemplateStep template = new TemplateStep();
    template.setId("template");
    template.setStepInputs(Map.of("writes", Collections.singletonList(step)));
    Workflow workflow = Workflow.builder().steps(Arrays.asList(step, template)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals("[workflow step definitions] contain duplicate step ids", violation.getMessage());
  }

  @Test
  public void isTemplateStepInputsEmpty() {
    TemplateStep template = new TemplateStep();
    template.setId("template");
    template.setStepInputs(Collections.emptyMap());
    Workflow workflow = Workflow.builder().steps(Collections.singletonList(template)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step definitions] template step [template] step_inputs cannot be empty",
        violation.getMessage());
  }

  @Test
  public void isTemplateStepInputPositionEmpty() {
    TemplateStep template = new TemplateStep();
    template.setId("template");
    template.setStepInputs(Map.of("writes", Collections.emptyList()));
    Workflow workflow = Workflow.builder().steps(Collections.singletonList(template)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step definitions] template step [template] step_inputs position [writes] "
            + "cannot be empty",
        violation.getMessage());
  }

  /** An attached step may only name steps of its own position, never a template step id. */
  @Test
  public void isTemplateStepInputNamingOutsidePosition() {
    TypedStep step = new TypedStep();
    step.setId("validate");
    StepTransition transition = new StepTransition();
    transition.setSuccessors(Collections.singletonMap("audit", "true"));
    step.setTransition(transition);
    TemplateStep template = new TemplateStep();
    template.setId("template");
    template.setStepInputs(Map.of("writes", Collections.singletonList(step)));
    Workflow workflow = Workflow.builder().steps(Collections.singletonList(template)).build();
    Set<ConstraintViolation<TestWorkflow>> violations =
        validator.validate(new TestWorkflow(workflow));
    assertEquals(1, violations.size());
    ConstraintViolation<TestWorkflow> violation = violations.iterator().next();
    assertEquals("workflow.steps", violation.getPropertyPath().toString());
    assertEquals(
        "[workflow step definitions] template step [template] step input [validate] at position "
            + "[writes] names [audit], which is not a step of the same position",
        violation.getMessage());
  }

  private static class TestValidTemplateStep {
    @Valid TemplateStep step;

    TestValidTemplateStep(TemplateStep step) {
      this.step = step;
    }
  }

  @Test
  public void isTemplateStepInputIdReserved() {
    TypedStep step = new TypedStep();
    step.setId(Constants.TEMPLATE_OUTPUTS_STEP_ID);
    step.setType(StepType.NOOP);
    TemplateStep template = new TemplateStep();
    template.setId("template");
    template.setStepInputs(Map.of("writes", Collections.singletonList(step)));
    Set<ConstraintViolation<TestValidTemplateStep>> violations =
        validator.validate(new TestValidTemplateStep(template));
    assertEquals(1, violations.size());
    ConstraintViolation<TestValidTemplateStep> violation = violations.iterator().next();
    String path = violation.getPropertyPath().toString();
    assertTrue(
        path, path.startsWith("step.stepInputs[writes].<map value>[0]") && path.endsWith(".id"));
    assertEquals(
        "[maestro id or name reference] cannot start with reserved prefix: maestro_ "
            + "- rejected value is [maestro_template_outputs]",
        violation.getMessage());
  }

  /** Attached steps of one position form their own DAG; position names are resolved at run time. */
  @Test
  public void isTemplateStepInputDagWithinPositionValid() {
    TypedStep writeA = new TypedStep();
    writeA.setId("write_a");
    StepTransition transition = new StepTransition();
    transition.setSuccessors(Collections.singletonMap("merge", "true"));
    writeA.setTransition(transition);
    TypedStep writeB = new TypedStep();
    writeB.setId("write_b");
    TypedStep merge = new TypedStep();
    merge.setId("merge");
    StepTransition transitionMerge = new StepTransition();
    transitionMerge.setPredecessors(Collections.singletonList("write_b"));
    merge.setTransition(transitionMerge);
    TypedStep announce = new TypedStep();
    announce.setId("announce");
    TemplateStep template = new TemplateStep();
    template.setId("template");
    template.setStepInputs(
        Map.of(
            "writes",
            Arrays.asList(writeA, writeB, merge),
            "after_publish",
            Collections.singletonList(announce)));
    Workflow workflow = Workflow.builder().steps(Collections.singletonList(template)).build();
    assertTrue(validator.validate(new TestWorkflow(workflow)).isEmpty());
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
