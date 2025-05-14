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

import com.netflix.maestro.models.signal.SignalOutputsDefinition;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Objects;

/** Maestro signal step outputs definition validation. */
@Documented
@Constraint(validatedBy = SignalOutputsDefinitionConstraint.SignalOutputsDefinitionValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface SignalOutputsDefinitionConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  class SignalOutputsDefinitionValidator
      implements ConstraintValidator<SignalOutputsDefinitionConstraint, SignalOutputsDefinition> {
    @Override
    public boolean isValid(
        SignalOutputsDefinition signalOutputsDefinition, ConstraintValidatorContext context) {
      if (signalOutputsDefinition == null) {
        return true;
      }
      if (definitionsAreNullOrEmpty(signalOutputsDefinition, context)) {
        return false;
      }

      return !nameIsAbsent(signalOutputsDefinition, context);
    }

    private static boolean definitionsAreNullOrEmpty(
        SignalOutputsDefinition def, ConstraintValidatorContext context) {
      if (def.definitions() == null
          || def.definitions().isEmpty()
          || def.definitions().stream().anyMatch(Objects::isNull)) {
        context
            .buildConstraintViolationWithTemplate(
                "Signal step outputs definitions cannot be null or empty or contain null elements")
            .addConstraintViolation();
        return true;
      }
      return false;
    }

    private static boolean nameIsAbsent(
        SignalOutputsDefinition def, ConstraintValidatorContext context) {
      if (def.definitions().stream().anyMatch(d -> d.getName() == null || d.getName().isEmpty())) {
        context
            .buildConstraintViolationWithTemplate(
                "Signal step outputs definition doesn't contain mandatory name field")
            .addConstraintViolation();
        return true;
      }
      return false;
    }
  }
}
