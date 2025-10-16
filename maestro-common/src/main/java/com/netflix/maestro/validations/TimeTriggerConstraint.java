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

import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.trigger.TimeTrigger;
import com.netflix.maestro.utils.TriggerHelper;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.Optional;

/**
 * TimeTrigger constraint.
 *
 * <p>This validates {@link TimeTrigger} definition.
 */
@Documented
@Constraint(validatedBy = TimeTriggerConstraint.TimeTriggerConstraintValidator.class)
@Target({ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TimeTriggerConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro workflow TimeTrigger duration validator. */
  class TimeTriggerConstraintValidator
      implements ConstraintValidator<TimeTriggerConstraint, TimeTrigger> {

    @Override
    public void initialize(TimeTriggerConstraint constraint) {}

    @Override
    @SuppressWarnings("PMD.ReplaceJavaUtilDate")
    public boolean isValid(TimeTrigger trigger, ConstraintValidatorContext context) {
      try {
        Optional<Date> d1 = TriggerHelper.nextExecutionDate(trigger, new Date(), "");
        if (d1.isEmpty()) {
          return true;
        }
        Optional<Date> d2 = TriggerHelper.nextExecutionDate(trigger, d1.get(), "");
        if (d2.isEmpty()) {
          return true;
        }

        long period = d2.get().getTime() - d1.get().getTime();

        if (period < Constants.TIME_TRIGGER_MINIMUM_INTERVAL) {
          context
              .buildConstraintViolationWithTemplate(
                  String.format(
                      "[time-trigger] the interval between time triggers is less than the minimal value [%s] millis",
                      Constants.TIME_TRIGGER_MINIMUM_INTERVAL))
              .addConstraintViolation();
          return false;
        }
        return true;
      } catch (Exception e) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[time-trigger] is not valid - rejected value is [%s] - error: [%s]",
                    trigger, e.getMessage()))
            .addConstraintViolation();
        return false;
      }
    }
  }
}
