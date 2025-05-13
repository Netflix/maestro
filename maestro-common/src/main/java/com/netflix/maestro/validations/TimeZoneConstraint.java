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

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.joda.time.DateTimeZone;

/** Maestro timezone expression validation. */
@Documented
@Constraint(validatedBy = TimeZoneConstraint.TimeZoneValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface TimeZoneConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro timezone validator. */
  class TimeZoneValidator implements ConstraintValidator<TimeZoneConstraint, String> {
    @Override
    public void initialize(TimeZoneConstraint constraint) {}

    @Override
    public boolean isValid(String timezone, ConstraintValidatorContext context) {
      if (timezone == null || timezone.isEmpty()) {
        return true;
      }

      try {
        DateTimeZone.forID(timezone);
      } catch (IllegalArgumentException e) {
        context
            .buildConstraintViolationWithTemplate("[timezone expression] is not valid: " + e)
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
