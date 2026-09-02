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

import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Job template definition validation. A step list is required for the template step type and
 * rejected for every other step type. The step list follows the same rules as a workflow step list.
 */
@Documented
@Constraint(validatedBy = JobTemplateDefinitionConstraint.JobTemplateDefinitionValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface JobTemplateDefinitionConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro job template definition validator. */
  class JobTemplateDefinitionValidator
      implements ConstraintValidator<JobTemplateDefinitionConstraint, JobTemplate.Definition> {
    private static final String STEPS_PROPERTY_NAME = "steps";

    @Override
    public void initialize(JobTemplateDefinitionConstraint constraint) {}

    @Override
    public boolean isValid(JobTemplate.Definition definition, ConstraintValidatorContext context) {
      if (definition == null) {
        return true; // handled by @NotNull
      }
      if (definition.getStepType() != StepType.TEMPLATE) {
        if (definition.getSteps() != null) {
          context
              .buildConstraintViolationWithTemplate(
                  String.format(
                      "[job template step definitions] can only be set for the template step type "
                          + "- rejected step type is [%s]",
                      definition.getStepType()))
              .addPropertyNode(STEPS_PROPERTY_NAME)
              .addConstraintViolation();
          return false;
        }
        return true;
      }
      if (definition.getSteps() == null || definition.getSteps().isEmpty()) {
        context
            .buildConstraintViolationWithTemplate(
                "[job template step definitions] cannot be null or empty for the template step type")
            .addPropertyNode(STEPS_PROPERTY_NAME)
            .addConstraintViolation();
        return false;
      }
      // the step list is run as an inline workflow, so it must be a valid workflow step list.
      Workflow inlineWorkflow = Workflow.builder().steps(definition.getSteps()).build();
      return new WorkflowConstraint.WorkflowValidator().isValid(inlineWorkflow, context);
    }
  }
}
