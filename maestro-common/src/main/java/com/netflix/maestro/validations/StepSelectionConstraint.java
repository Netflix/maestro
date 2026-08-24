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

import com.netflix.maestro.models.instance.StepSelection;
import com.netflix.maestro.models.instance.StepSelector;
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
 * Rejects an {@code include} or {@code exclude} that carries no criteria, as such a selector is
 * ambiguous between every step and no step. Callers omit the field instead.
 */
@Documented
@Constraint(validatedBy = StepSelectionConstraint.StepSelectionValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepSelectionConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Step selection validator. */
  class StepSelectionValidator
      implements ConstraintValidator<StepSelectionConstraint, StepSelection> {

    @Override
    public void initialize(StepSelectionConstraint constraint) {}

    @Override
    public boolean isValid(StepSelection selection, ConstraintValidatorContext context) {
      if (selection == null) {
        return true;
      }
      return isSelectorValid(selection.getInclude(), "include", context)
          && isSelectorValid(selection.getExclude(), "exclude", context);
    }

    private static boolean isSelectorValid(
        StepSelector selector, String field, ConstraintValidatorContext context) {
      if (selector != null && selector.isEmpty()) {
        return reject(
            context,
            "[step selection] "
                + field
                + " must set at least one step id, prefix, infix or suffix");
      }
      return true;
    }

    private static boolean reject(ConstraintValidatorContext context, String message) {
      context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
      return false;
    }
  }
}
