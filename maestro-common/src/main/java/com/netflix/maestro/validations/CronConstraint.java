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

import com.cronutils.model.Cron;
import com.netflix.maestro.utils.TriggerHelper;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

/** Maestro cron expression validation. */
@Documented
@Constraint(validatedBy = CronConstraint.CronValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface CronConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro cron validator. */
  class CronValidator implements ConstraintValidator<CronConstraint, String> {
    @Override
    public void initialize(CronConstraint constraint) {}

    @Override
    public boolean isValid(String cronExpression, ConstraintValidatorContext context) {
      if (cronExpression == null || cronExpression.isEmpty()) {
        context
            .buildConstraintViolationWithTemplate("[cron expression] cannot be null or empty")
            .addConstraintViolation();
        return false;
      }
      Cron cron = TriggerHelper.buildCron(cronExpression);
      if (cron == null) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[cron expression] is not valid - rejected value is [%s]", cronExpression))
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
