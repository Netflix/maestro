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

import com.netflix.maestro.models.definition.Step;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Step timeout validation: a step sets either the single timeout or the per-phase timeouts. */
@Documented
@Constraint(validatedBy = StepTimeoutConstraint.StepTimeoutValidator.class)
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface StepTimeoutConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Step timeout validator. */
  class StepTimeoutValidator implements ConstraintValidator<StepTimeoutConstraint, Step> {
    @Override
    public boolean isValid(Step step, ConstraintValidatorContext context) {
      if (step != null && step.getTimeout() != null && step.getTimeouts() != null) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[step timeout] step [%s] cannot set both [timeout] and [timeouts], use one of them",
                    step.getId()))
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
