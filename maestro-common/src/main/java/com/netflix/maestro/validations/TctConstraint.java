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
import com.netflix.maestro.models.definition.Tct;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Workflow version validation. */
@Documented
@Constraint(validatedBy = TctConstraint.TctValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface TctConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro workflow validator. */
  class TctValidator implements ConstraintValidator<TctConstraint, Tct> {
    private static final int VALID_TIME_FIELD_COUNT = 1;

    @Override
    public void initialize(TctConstraint constraint) {}

    @Override
    public boolean isValid(Tct tct, ConstraintValidatorContext context) {
      if (tct != null) {
        int count =
            (tct.getCompletedByHour() == null ? 0 : 1)
                + (tct.getCompletedByTs() == null ? 0 : 1)
                + (tct.getDurationMinutes() == null ? 0 : 1);
        if (count != VALID_TIME_FIELD_COUNT) {
          context
              .buildConstraintViolationWithTemplate(
                  "[TCT definition] is invalid, one and only one time field has to be set: " + tct)
              .addConstraintViolation();
          return false;
        }
        try {
          tct.getCompletedByTsParam();
        } catch (MaestroInternalError e) {
          context
              .buildConstraintViolationWithTemplate(
                  "[TCT definition] is invalid, fail to parse completedByTs due to error: "
                      + e.getMessage())
              .addConstraintViolation();
          return false;
        }
      }
      return true;
    }
  }
}
