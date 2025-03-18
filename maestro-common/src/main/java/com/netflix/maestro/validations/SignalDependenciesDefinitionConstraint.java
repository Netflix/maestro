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

import com.netflix.maestro.models.signal.SignalDependenciesDefinition;
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

/** Maestro signal step dependencies definition validation. */
@Documented
@Constraint(
    validatedBy = SignalDependenciesDefinitionConstraint.StepDependenciesDefinitionValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface SignalDependenciesDefinitionConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  class StepDependenciesDefinitionValidator
      implements ConstraintValidator<
          SignalDependenciesDefinitionConstraint, SignalDependenciesDefinition> {
    @Override
    public boolean isValid(
        SignalDependenciesDefinition signalDependenciesDefinition,
        ConstraintValidatorContext context) {
      if (signalDependenciesDefinition == null) {
        return true;
      }
      if (definitionsAreNullOrEmpty(signalDependenciesDefinition, context)) {
        return false;
      }
      if (nameIsAbsent(signalDependenciesDefinition, context)) {
        return false;
      }

      return !moreThanOneRangeParamExists(signalDependenciesDefinition, context);
    }

    private static boolean definitionsAreNullOrEmpty(
        SignalDependenciesDefinition signalDependenciesDefinition,
        ConstraintValidatorContext context) {
      if (signalDependenciesDefinition.definitions() == null
          || signalDependenciesDefinition.definitions().isEmpty()
          || signalDependenciesDefinition.definitions().stream().anyMatch(Objects::isNull)) {
        context
            .buildConstraintViolationWithTemplate(
                "signal dependencies definitions cannot be null or empty or contain null elements")
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private static boolean nameIsAbsent(
        SignalDependenciesDefinition signalDependenciesDefinition,
        ConstraintValidatorContext context) {
      if (signalDependenciesDefinition.definitions().stream()
          .anyMatch(d -> d.getName() == null || d.getName().isEmpty())) {
        context
            .buildConstraintViolationWithTemplate(
                "Signal step dependency definition doesn't contain mandatory name field")
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private static boolean moreThanOneRangeParamExists(
        SignalDependenciesDefinition signalDependenciesDefinition,
        ConstraintValidatorContext context) {
      for (var def : signalDependenciesDefinition.definitions()) {
        if (def.getMatchParams() != null) {
          String paramName = null;
          for (var entry : def.getMatchParams().entrySet()) {
            if (entry.getValue().getOperator().isRangeParam()) {
              if (paramName != null) {
                context
                    .buildConstraintViolationWithTemplate(
                        String.format(
                            "Invalid signal dependency for signal [%s] with more than one range params: [%s] and [%s]",
                            def.getName(), paramName, entry.getKey()))
                    .addConstraintViolation();
                return true;
              }
              paramName = entry.getKey();
            }
          }
        }
      }
      return false;
    }
  }
}
