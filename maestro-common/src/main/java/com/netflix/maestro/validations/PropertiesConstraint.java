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

import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.Constants;
import com.netflix.maestro.models.definition.Alerting;
import com.netflix.maestro.models.definition.Properties;
import com.netflix.maestro.models.definition.RunStrategy;
import jakarta.inject.Inject;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Workflow properties validation. */
@Documented
@Constraint(validatedBy = PropertiesConstraint.PropertiesValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface PropertiesConstraint {
  /** input constraint message. */
  String message() default "";

  /** input constraint groups. */
  Class<?>[] groups() default {};

  /** input constraint payload. */
  Class<? extends Payload>[] payload() default {};

  /** Maestro properties validator. */
  class PropertiesValidator implements ConstraintValidator<PropertiesConstraint, Properties> {

    @Inject private AlertingValidator alertingValidator;

    @Override
    public void initialize(PropertiesConstraint constraint) {}

    @Override
    public boolean isValid(Properties properties, ConstraintValidatorContext context) {
      return properties == null
          || (isAlertingValid(properties.getAlerting(), context)
              && isRunStrategyValid(properties.getRunStrategy(), context)
              && isStepConcurrencyValid(properties.getStepConcurrency(), context));
    }

    private boolean isAlertingValid(
        @Nullable Alerting alerting, ConstraintValidatorContext context) {
      return alertingValidator == null || alertingValidator.isValid(alerting, context);
    }

    private boolean isRunStrategyValid(
        RunStrategy runStrategy, ConstraintValidatorContext context) {
      if (runStrategy != null
          && (runStrategy.getWorkflowConcurrency() < 1
              || runStrategy.getWorkflowConcurrency() > Constants.WORKFLOW_CONCURRENCY_MAX_LIMIT)) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[workflow properties run_strategy] workflow_concurrency should be positive and no "
                        + "greater than %s - rejected value is [%s]",
                    Constants.WORKFLOW_CONCURRENCY_MAX_LIMIT, runStrategy.getWorkflowConcurrency()))
            .addPropertyNode("runStrategy.workflowConcurrency")
            .addConstraintViolation();
        return false;
      }
      return true;
    }

    private boolean isStepConcurrencyValid(
        Long stepConcurrency, ConstraintValidatorContext context) {
      if (stepConcurrency != null
          && (stepConcurrency < 1 || stepConcurrency > Constants.STEP_CONCURRENCY_MAX_LIMIT)) {
        context
            .buildConstraintViolationWithTemplate(
                String.format(
                    "[workflow properties] step_concurrency should be positive and no greater than %s "
                        + "- rejected value is [%s]",
                    Constants.STEP_CONCURRENCY_MAX_LIMIT, stepConcurrency))
            .addPropertyNode("stepConcurrency")
            .addConstraintViolation();
        return false;
      }
      return true;
    }
  }
}
