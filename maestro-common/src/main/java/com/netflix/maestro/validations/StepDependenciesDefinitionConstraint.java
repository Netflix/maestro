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

import com.netflix.maestro.models.definition.StepDependenciesDefinition;
import com.netflix.maestro.models.definition.StepDependencyType;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.models.parameter.SignalParamDefinition;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Objects;

@Documented
@Constraint(
    validatedBy = StepDependenciesDefinitionConstraint.StepDependenciesDefinitionValidator.class)
@Target({ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepDependenciesDefinitionConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  class StepDependenciesDefinitionValidator
      implements ConstraintValidator<
          StepDependenciesDefinitionConstraint, StepDependenciesDefinition> {
    @Override
    public boolean isValid(
        StepDependenciesDefinition stepDependenciesDefinition, ConstraintValidatorContext context) {
      if (definitionsAreNullOrEmpty(stepDependenciesDefinition, context)) {
        return false;
      }

      if (stepDependenciesDefinition.getType() == StepDependencyType.SIGNAL) {
        if (nameParamIsAbsent(stepDependenciesDefinition, context)) {
          return false;
        }

        if (moreThanOneRangeParamExists(stepDependenciesDefinition, context)) {
          return false;
        }
      }
      return true;
    }

    private static boolean definitionsAreNullOrEmpty(
        StepDependenciesDefinition stepDependenciesDefinition, ConstraintValidatorContext context) {
      if (stepDependenciesDefinition.getDefinitions() == null
          || stepDependenciesDefinition.getDefinitions().isEmpty()
          || stepDependenciesDefinition.getDefinitions().stream().anyMatch(Objects::isNull)) {
        context
            .buildConstraintViolationWithTemplate(
                "signal dependencies definitions cannot be null or empty or contain null elements")
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private static boolean nameParamIsAbsent(
        StepDependenciesDefinition stepDependenciesDefinition, ConstraintValidatorContext context) {
      if (stepDependenciesDefinition.getDefinitions().stream()
          .anyMatch(
              d -> !d.getValue().containsKey(StepDependenciesDefinition.STEP_DEPENDENCY_NAME))) {
        context
            .buildConstraintViolationWithTemplate(
                "step dependency definition doesn't contain mandatory name parameter definition")
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private static boolean moreThanOneRangeParamExists(
        StepDependenciesDefinition stepDependenciesDefinition, ConstraintValidatorContext context) {
      for (MapParamDefinition entry : stepDependenciesDefinition.getDefinitions()) {
        boolean rangeParamCondition = false;
        String paramName = "";
        for (Map.Entry<String, ParamDefinition> paramDefEntry : entry.getValue().entrySet()) {
          if (paramDefEntry.getValue() instanceof SignalParamDefinition) {
            SignalParamDefinition signalParamDefinition =
                (SignalParamDefinition) paramDefEntry.getValue();
            if (signalParamDefinition.getOperator().isRangeParam()) {
              if (rangeParamCondition) {
                context
                    .buildConstraintViolationWithTemplate(
                        "signal dependencies can not have two range params, "
                            + String.format(
                                "signal name [%s] has two parameters [%s] and [%s] using range operators",
                                entry
                                    .getValue()
                                    .get(StepDependenciesDefinition.STEP_DEPENDENCY_NAME)
                                    .getValue(),
                                paramName,
                                paramDefEntry.getKey()))
                    .addConstraintViolation();
                return true;
              }
              rangeParamCondition = true;
              paramName = paramDefEntry.getKey();
            }
          }
        }
      }
      return false;
    }
  }
}
