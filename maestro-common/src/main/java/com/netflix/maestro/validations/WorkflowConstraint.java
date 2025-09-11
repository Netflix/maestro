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

import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.ForeachStep;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepTransition;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.WhileStep;
import com.netflix.maestro.models.definition.Workflow;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Workflow definition validation. */
@Documented
@Constraint(validatedBy = WorkflowConstraint.WorkflowValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface WorkflowConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro workflow validator. */
  class WorkflowValidator implements ConstraintValidator<WorkflowConstraint, Workflow> {
    private static final String STEPS_PROPERTY_NAME = "steps";

    @Override
    public void initialize(WorkflowConstraint constraint) {}

    @Override
    public boolean isValid(Workflow workflow, ConstraintValidatorContext context) {
      if (workflow == null) {
        context
            .buildConstraintViolationWithTemplate("[workflow definition] cannot be null")
            .addConstraintViolation();
        return false;
      }

      return isStepListValid(workflow, context)
          && isDagValid(workflow.getSteps(), context)
          && isInstanceStepConcurrencyValid(workflow.getInstanceStepConcurrency(), context)
          && isLoopParamNameValid(workflow, context);
    }

    private boolean isStepListValid(Workflow workflow, ConstraintValidatorContext context) {
      List<Step> steps = workflow.getSteps();
      if (steps == null || steps.isEmpty()) {
        context
            .buildConstraintViolationWithTemplate(
                "[workflow step definitions] cannot be null or empty")
            .addPropertyNode(STEPS_PROPERTY_NAME)
            .addConstraintViolation();
        return false;
      }

      // recursively go over the whole step definition to count the total step number.
      final List<String> stepList = workflow.getAllStepIds();
      if (stepList.size() > Constants.STEP_LIST_SIZE_LIMIT) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[workflow step list] is too large [%s] and over the size limit [%s]",
                    stepList.size(), Constants.STEP_LIST_SIZE_LIMIT))
            .addPropertyNode(STEPS_PROPERTY_NAME)
            .addConstraintViolation();
        return false;
      }
      if (stepList.size() != new HashSet<>(stepList).size()) {
        context
            .buildConstraintViolationWithTemplate(
                "[workflow step definitions] contain duplicate step ids")
            .addPropertyNode(STEPS_PROPERTY_NAME)
            .addConstraintViolation();
        return false;
      }
      return true;
    }

    @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops"})
    private boolean isDagValid(List<Step> steps, ConstraintValidatorContext context) {
      Map<String, StepTransition> transitionMap =
          steps.stream().collect(Collectors.toMap(Step::getId, Step::getTransition));
      Map<String, Set<String>> predecessors = new HashMap<>();

      for (Step step : steps) {
        String parent = step.getId();
        for (String child : step.getTransition().getSuccessors().keySet()) {
          if (!transitionMap.containsKey(child)) {
            context
                .buildConstraintViolationWithTemplate(
                    String.format(
                        "[workflow step transition] is invalid for step [%s]'s successor step id [%s], "
                            + "which does not exist in steps",
                        parent, child))
                .addPropertyNode(STEPS_PROPERTY_NAME)
                .addConstraintViolation();
            return false;
          }
          if (!predecessors.containsKey(child)) {
            predecessors.put(child, new HashSet<>());
          }
          if (predecessors.get(child).contains(parent)) {
            throw new MaestroInternalError(
                "Finding step [%s] has step [%s] as predecessor twice. This should not happen",
                child, parent);
          }
          predecessors.get(child).add(parent);
        }
      }
      for (Step step : steps) {
        if (!step.getTransition().getPredecessors().isEmpty()
            && !new HashSet<>(step.getTransition().getPredecessors())
                .equals(predecessors.get(step.getId()))) {
          context
              .buildConstraintViolationWithTemplate(
                  String.format(
                      "[workflow step transition] is invalid for step [%s]'s predecessors list [%s], "
                          + "which does match computed one [%s] based on the DAG",
                      step.getId(),
                      step.getTransition().getPredecessors(),
                      predecessors.get(step.getId())))
              .addPropertyNode(STEPS_PROPERTY_NAME)
              .addConstraintViolation();
          return false;
        }
      }
      return true;
    }

    private boolean isInstanceStepConcurrencyValid(
        Long instanceStepConcurrency, ConstraintValidatorContext context) {
      if (instanceStepConcurrency != null
          && (instanceStepConcurrency < 1
              || instanceStepConcurrency >= Constants.INSTANCE_STEP_CONCURRENCY_MAX_LIMIT)) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[workflow definitions] instance_step_concurrency "
                        + "should be positive and less than %s - rejected value is [%s]",
                    Constants.INSTANCE_STEP_CONCURRENCY_MAX_LIMIT, instanceStepConcurrency))
            .addPropertyNode("instanceStepConcurrency")
            .addConstraintViolation();
        return false;
      }
      return true;
    }

    private boolean isLoopParamNameValid(Workflow workflow, ConstraintValidatorContext context) {
      return !hasDuplicateLoopParamNames(workflow.getSteps(), context, new HashSet<>());
    }

    private boolean hasDuplicateLoopParamNames(
        List<Step> steps, ConstraintValidatorContext context, Set<String> loopParamNames) {
      for (Step step : steps) {
        if (step.getType() == StepType.FOREACH || step.getType() == StepType.WHILE) {
          Set<String> currentLoopNames =
              step.getParams().get(Constants.LOOP_PARAMS_NAME).asMapParamDef().getValue().keySet();
          for (String name : currentLoopNames) {
            if (!loopParamNames.add(name)) {
              // if the name already appears in upper level loop params, duplicate names detected.
              context
                  .buildConstraintViolationWithTemplate(
                      String.format(
                          "[workflow step definitions] contain duplicate loop param name [%s]",
                          name))
                  .addPropertyNode(STEPS_PROPERTY_NAME)
                  .addConstraintViolation();
              return true;
            }
          }
          // recursively pass down all upper level loop param names to nested loop.
          List<Step> nestedSteps =
              step.getType() == StepType.FOREACH
                  ? ((ForeachStep) step).getSteps()
                  : ((WhileStep) step).getSteps();
          if (hasDuplicateLoopParamNames(nestedSteps, context, loopParamNames)) {
            return true;
          }
          // remove all param names from current foreach step.
          currentLoopNames.forEach(loopParamNames::remove);
        }
      }
      return false;
    }
  }
}
