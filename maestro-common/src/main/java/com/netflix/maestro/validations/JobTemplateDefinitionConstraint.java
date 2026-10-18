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

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.definition.StepType;
import com.netflix.maestro.models.definition.Workflow;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

/**
 * Job template definition validation. The template fields {@code prepare} and {@code output_params}
 * are required or allowed only for the template step type and rejected for every other step type. A
 * static step list follows the same rules as a workflow step list. Output param names may not
 * collide with the template params or the reserved expansion param.
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
    private static final String PREPARE_PROPERTY_NAME = "prepare";
    private static final String OUTPUT_PARAMS_PROPERTY_NAME = "output_params";

    @Override
    public void initialize(JobTemplateDefinitionConstraint constraint) {}

    @Override
    public boolean isValid(JobTemplate.Definition definition, ConstraintValidatorContext context) {
      if (definition == null) {
        return true; // handled by @NotNull
      }
      if (definition.getStepType() != StepType.TEMPLATE) {
        return isTemplateFieldsAbsent(definition, context);
      }
      TemplatePrepare prepare = definition.getPrepare();
      if (prepare == null) {
        return violation(
            context,
            "[job template prepare] cannot be null for the template step type",
            PREPARE_PROPERTY_NAME);
      }
      return isOutputParamsValid(definition, context) && isPrepareValid(definition, context);
    }

    private boolean isTemplateFieldsAbsent(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      String rejected = null;
      if (definition.getPrepare() != null) {
        rejected = PREPARE_PROPERTY_NAME;
      } else if (definition.getOutputParams() != null) {
        rejected = OUTPUT_PARAMS_PROPERTY_NAME;
      }
      if (rejected != null) {
        return violation(
            context,
            String.format(
                "[job template %s] can only be set for the template step type "
                    + "- rejected step type is [%s]",
                rejected, definition.getStepType()),
            rejected);
      }
      return true;
    }

    private boolean isOutputParamsValid(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      if (definition.getOutputParams() == null) {
        return true;
      }
      for (String name : definition.getOutputParams().keySet()) {
        if (Constants.TEMPLATE_EXPANSION_PARAM.equals(name)) {
          return violation(
              context,
              String.format(
                  "[job template output params] cannot use the reserved param name [%s]", name),
              OUTPUT_PARAMS_PROPERTY_NAME);
        }
        if (definition.getParams() != null && definition.getParams().containsKey(name)) {
          return violation(
              context,
              String.format(
                  "[job template output params] param name [%s] collides with a template param",
                  name),
              OUTPUT_PARAMS_PROPERTY_NAME);
        }
      }
      return true;
    }

    private boolean isPrepareValid(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      TemplatePrepare prepare = definition.getPrepare();
      if (prepare.getType() == null) {
        return violation(
            context, "[job template prepare] type cannot be null", PREPARE_PROPERTY_NAME);
      }
      return switch (prepare.getType()) {
        case TemplatePrepare.STATIC -> isStaticPrepareValid(definition, context);
        case TemplatePrepare.SEL -> isSelPrepareValid(prepare, context);
        case TemplatePrepare.STEP -> isStepPrepareValid(prepare, context);
        default -> true; // an unknown type is checked against the enabled preparers by the engine
      };
    }

    private boolean isStaticPrepareValid(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      List<Step> steps = definition.getPrepare().getSteps();
      if (steps == null || steps.isEmpty()) {
        return violation(
            context,
            "[job template prepare] steps cannot be null or empty for the static preparer",
            PREPARE_PROPERTY_NAME);
      }
      // the step list is run as an inline workflow, so it must be a valid workflow step list.
      Workflow inlineWorkflow = Workflow.builder().steps(steps).build();
      return new WorkflowConstraint.WorkflowValidator().isValid(inlineWorkflow, context);
    }

    private boolean isSelPrepareValid(TemplatePrepare prepare, ConstraintValidatorContext context) {
      if (prepare.getExpression() == null || prepare.getExpression().isBlank()) {
        return violation(
            context,
            "[job template prepare] expression cannot be null or empty for the sel preparer",
            PREPARE_PROPERTY_NAME);
      }
      return true;
    }

    private boolean isStepPrepareValid(
        TemplatePrepare prepare, ConstraintValidatorContext context) {
      if (prepare.getStep() == null) {
        return violation(
            context,
            "[job template prepare] step cannot be null for the step preparer",
            PREPARE_PROPERTY_NAME);
      }
      if (prepare.getStep().getType() == StepType.TEMPLATE) {
        return violation(
            context,
            "[job template prepare] generator step cannot be a template step",
            PREPARE_PROPERTY_NAME);
      }
      if (prepare.getStep().getParams() != null
          && prepare.getStep().getParams().containsKey(Constants.TEMPLATE_EXPANSION_PARAM)) {
        return violation(
            context,
            String.format(
                "[job template prepare] generator step cannot declare the reserved param [%s]",
                Constants.TEMPLATE_EXPANSION_PARAM),
            PREPARE_PROPERTY_NAME);
      }
      return true;
    }

    private static boolean violation(
        ConstraintValidatorContext context, String message, String propertyNode) {
      context
          .buildConstraintViolationWithTemplate(message)
          .addPropertyNode(propertyNode)
          .addConstraintViolation();
      return false;
    }
  }
}
