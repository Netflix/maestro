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

import com.netflix.maestro.models.definition.SignalOutputsDefinition;
import com.netflix.maestro.models.definition.StepDependenciesDefinition;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Objects;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

@Documented
@Constraint(validatedBy = StepOutputsDefinitionConstraint.StepOutputsDefinitionValidator.class)
@Target({ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepOutputsDefinitionConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  class StepOutputsDefinitionValidator
      implements ConstraintValidator<StepOutputsDefinitionConstraint, StepOutputsDefinition> {
    @Override
    public boolean isValid(
        StepOutputsDefinition stepOutputsDefinition, ConstraintValidatorContext context) {
      if (definitionsAreNullOrEmpty(stepOutputsDefinition, context)) {
        return false;
      }

      if (stepOutputsDefinition.getType() == StepOutputsDefinition.StepOutputType.SIGNAL) {
        if (nameParamIsAbsent(stepOutputsDefinition, context)) {
          return false;
        }
      }
      return true;
    }

    private static boolean definitionsAreNullOrEmpty(
        StepOutputsDefinition stepOutputsDefinition, ConstraintValidatorContext context) {
      if (stepOutputsDefinition.getType() == StepOutputsDefinition.StepOutputType.SIGNAL) {
        SignalOutputsDefinition def = stepOutputsDefinition.asSignalOutputsDefinition();
        if (def.getDefinitions() == null
            || def.getDefinitions().isEmpty()
            || def.getDefinitions().stream().anyMatch(Objects::isNull)) {
          context
              .buildConstraintViolationWithTemplate(
                  "step outputs definitions cannot be null or empty or contain null elements")
              .addConstraintViolation();
          return true;
        }
      }
      return false;
    }

    private static boolean nameParamIsAbsent(
        StepOutputsDefinition stepOutputsDefinition, ConstraintValidatorContext context) {
      if (stepOutputsDefinition.getType() == StepOutputsDefinition.StepOutputType.SIGNAL) {
        SignalOutputsDefinition def = stepOutputsDefinition.asSignalOutputsDefinition();
        if (def.getDefinitions().stream()
            .anyMatch(
                d -> !d.getValue().containsKey(StepDependenciesDefinition.STEP_DEPENDENCY_NAME))) {
          context
              .buildConstraintViolationWithTemplate(
                  "step outputs definition doesn't contain mandatory name parameter definition")
              .addConstraintViolation();
          return true;
        }
      }
      return false;
    }
  }
}
