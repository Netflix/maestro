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
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.ParamMode;
import com.netflix.maestro.models.parameter.ParamSource;
import com.netflix.maestro.models.parameter.ParamType;
import com.netflix.maestro.models.stepruntime.JobTemplate;
import com.netflix.maestro.models.stepruntime.TemplatePrepare;
import com.netflix.maestro.models.stepruntime.TemplateStepInput;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Job template definition validation. The template fields {@code prepare} and {@code step_inputs}
 * are required or allowed only for the template step type and rejected for every other step type. A
 * static step list follows the same rules as a workflow step list. A template param declared with
 * the {@code TEMPLATE_OUTPUT} source is what the template promises outward and must be {@code
 * IMMUTABLE} with an expression; no other source may be set. Every step input position must sit
 * somewhere, expose only template params, and collect only scalar outputs.
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
    private static final String PARAMS_PROPERTY_NAME = "params";
    private static final String STEP_INPUTS_PROPERTY_NAME = "step_inputs";
    private static final String STEP_INPUTS_PREFIX = "[job template step_inputs] position [%s] ";

    /** Output types a position may collect; SEL arrays are one-dimensional. */
    private static final Set<ParamType> OUTPUT_TYPES =
        EnumSet.of(ParamType.STRING, ParamType.LONG, ParamType.DOUBLE, ParamType.BOOLEAN);

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
      return isParamsValid(definition, context)
          && isPrepareValid(definition, context)
          && isStepInputsValid(definition, context);
    }

    private boolean isTemplateFieldsAbsent(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      String rejected = null;
      if (definition.getPrepare() != null) {
        rejected = PREPARE_PROPERTY_NAME;
      } else if (definition.getStepInputs() != null) {
        rejected = STEP_INPUTS_PROPERTY_NAME;
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

    private boolean isParamsValid(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      if (definition.getParams() == null) {
        return true;
      }
      for (Map.Entry<String, ParamDefinition> entry : definition.getParams().entrySet()) {
        ParamDefinition def = entry.getValue();
        if (def.getSource() == null) {
          continue;
        }
        if (def.getSource() != ParamSource.TEMPLATE_OUTPUT) {
          return violation(
              context,
              String.format(
                  "[job template params] param [%s] cannot set the source [%s], "
                      + "only TEMPLATE_OUTPUT is allowed for a param the template promises outward",
                  entry.getKey(), def.getSource()),
              PARAMS_PROPERTY_NAME);
        }
        if (def.getMode() != ParamMode.IMMUTABLE || def.getExpression() == null) {
          return violation(
              context,
              String.format(
                  "[job template params] promised param [%s] with the TEMPLATE_OUTPUT source "
                      + "must be in IMMUTABLE mode and carry an expression",
                  entry.getKey()),
              PARAMS_PROPERTY_NAME);
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
      if (!isUnusedPrepareFieldAbsent(prepare, context)) {
        return false;
      }
      return switch (prepare.getType()) {
        case TemplatePrepare.STATIC -> isStaticPrepareValid(definition, context);
        case TemplatePrepare.SEL -> isSelPrepareValid(prepare, context);
        case TemplatePrepare.STEP -> isStepPrepareValid(prepare, context);
        default -> true; // an unknown type is checked against the enabled preparers by the engine
      };
    }

    /** Each built-in preparer type reads one field; a field of another type is rejected. */
    private boolean isUnusedPrepareFieldAbsent(
        TemplatePrepare prepare, ConstraintValidatorContext context) {
      boolean builtIn =
          switch (prepare.getType()) {
            case TemplatePrepare.STATIC, TemplatePrepare.SEL, TemplatePrepare.STEP -> true;
            default -> false;
          };
      if (!builtIn) {
        return true;
      }
      if (prepare.getSteps() != null && !TemplatePrepare.STATIC.equals(prepare.getType())) {
        return unusedPrepareField(context, "steps", TemplatePrepare.STATIC);
      }
      if (prepare.getExpression() != null && !TemplatePrepare.SEL.equals(prepare.getType())) {
        return unusedPrepareField(context, "expression", TemplatePrepare.SEL);
      }
      if (prepare.getStep() != null && !TemplatePrepare.STEP.equals(prepare.getType())) {
        return unusedPrepareField(context, "step", TemplatePrepare.STEP);
      }
      return true;
    }

    private boolean unusedPrepareField(
        ConstraintValidatorContext context, String field, String ownerType) {
      return violation(
          context,
          String.format(
              "[job template prepare] %s can only be set for the %s preparer", field, ownerType),
          PREPARE_PROPERTY_NAME);
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

    /**
     * A position must name at least one predecessor or successor. For a static preparer every named
     * id must be a registered step and the position name must not be one. Exposed params must be
     * template params and collected outputs must be scalar. Positions of a dynamic preparer are
     * resolved against the expansion when the template step runs.
     */
    private boolean isStepInputsValid(
        JobTemplate.Definition definition, ConstraintValidatorContext context) {
      if (definition.getStepInputs() == null) {
        return true;
      }
      List<Step> staticSteps =
          TemplatePrepare.STATIC.equals(definition.getPrepare().getType())
              ? definition.getPrepare().getSteps()
              : null;
      Set<String> stepIds =
          staticSteps == null
              ? Collections.emptySet()
              : staticSteps.stream().map(Step::getId).collect(Collectors.toSet());
      Set<String> paramNames =
          definition.getParams() == null ? Collections.emptySet() : definition.getParams().keySet();
      for (Map.Entry<String, TemplateStepInput> entry : definition.getStepInputs().entrySet()) {
        String name = entry.getKey();
        TemplateStepInput position = entry.getValue();
        if (name.startsWith(Constants.MAESTRO_PREFIX)) {
          return stepInputsViolation(
              context, name, "cannot use the reserved prefix [%s]", Constants.MAESTRO_PREFIX);
        }
        if (stepIds.contains(name)) {
          return stepInputsViolation(context, name, "collides with a template step id");
        }
        List<String> named =
            Stream.concat(nullSafe(position.getPredecessors()), nullSafe(position.getSuccessors()))
                .toList();
        if (named.isEmpty()) {
          return stepInputsViolation(context, name, "must name a predecessor or a successor");
        }
        if (staticSteps != null) {
          for (String id : named) {
            if (!stepIds.contains(id)) {
              return stepInputsViolation(
                  context, name, "names [%s], which is not a template step", id);
            }
          }
        }
        if (position.getParams() != null) {
          for (String param : position.getParams()) {
            if (!paramNames.contains(param)) {
              return stepInputsViolation(
                  context, name, "exposes [%s], which is not a template param", param);
            }
          }
        }
        if (position.getOutputs() != null) {
          for (var output : position.getOutputs().entrySet()) {
            TemplateStepInput.Output decl = output.getValue();
            if (decl == null || !OUTPUT_TYPES.contains(decl.getType())) {
              return stepInputsViolation(
                  context,
                  name,
                  "output [%s] must be one of the types %s",
                  output.getKey(),
                  OUTPUT_TYPES);
            }
          }
        }
      }
      return true;
    }

    private static Stream<String> nullSafe(List<String> ids) {
      return ids == null ? Stream.empty() : ids.stream();
    }

    private static boolean stepInputsViolation(
        ConstraintValidatorContext context, String position, String message, Object... args) {
      return violation(
          context,
          String.format(STEP_INPUTS_PREFIX, position) + String.format(message, args),
          STEP_INPUTS_PROPERTY_NAME);
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
