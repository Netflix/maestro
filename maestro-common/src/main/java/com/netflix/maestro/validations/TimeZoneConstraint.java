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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.ZoneId;
import java.util.Set;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

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
    private static final Set<String> AVAILABLE_ZONE_IDS = ZoneId.getAvailableZoneIds();

    @Override
    public void initialize(TimeZoneConstraint constraint) {}

    @Override
    public boolean isValid(String timezone, ConstraintValidatorContext context) {
      if (timezone == null || timezone.isEmpty()) {
        return true;
      }

      if (!AVAILABLE_ZONE_IDS.contains(timezone)) {
        context
            .buildConstraintViolationWithTemplate("[timezone expression] is not valid: " + timezone)
            .addConstraintViolation();
        return false;
      }

      return true;
    }
  }
}
